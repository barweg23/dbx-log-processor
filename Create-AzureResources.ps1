#Requires -Version 7.0
<#
.SYNOPSIS
    Provisions every Azure resource needed by the "Databricks - Log processor" challenge.

.DESCRIPTION
    Creates, in an idempotent way:
      * a resource group
      * an ADLS Gen2 storage account (hierarchical namespace) with "raw" and "processed" containers
      * an Azure Databricks workspace (Premium SKU - required for Unity Catalog)
      * an Azure Databricks Access Connector with RBAC on the storage account,
        so the workspace can reach the data lake without storage account keys
      * an Azure SQL server + database and firewall rules

    The log tables themselves are NOT created here. Outbound TCP 1433 is blocked on many
    corporate networks, so seeding is a manual step in the challenge (task 02), run from the
    Azure Portal Query editor or Azure Cloud Shell using the scripts in ./sql.

    The script only uses Microsoft Entra / RBAC for data plane access. No storage account key is
    ever written to disk.

.PARAMETER SubscriptionId
    Target subscription. Defaults to the currently selected one.

.PARAMETER ResourceGroup
    Name of the resource group to create or reuse.

.PARAMETER Location
    Azure region, e.g. westeurope.

.PARAMETER SqlLocation
    One or more regions to try for the Azure SQL logical server, in order. Defaults to -Location
    followed by a built-in fallback list. Regions regularly reject new SQL servers with
    "RegionDoesNotAllowProvisioning", so the script moves on to the next candidate.

.PARAMETER NamePrefix
    Short prefix (3-11 lowercase alphanumeric characters) used to build resource names.

.PARAMETER SqlAdminUser
    SQL administrator login. Must not be a reserved name such as "admin" or "sa".

.PARAMETER SqlAdminPassword
    SQL administrator password as a SecureString. You are prompted if it is not supplied.

.EXAMPLE
    ./Create-AzureResources.ps1 -ResourceGroup rg-dbx-logs-demo -Location westeurope -NamePrefix dbxlogs

.EXAMPLE
    ./Create-AzureResources.ps1 -ResourceGroup rg-dbx-logs-demo -Location westeurope `
        -SqlLocation polandcentral,swedencentral -NamePrefix dbxlogs
#>
[CmdletBinding()]
param(
    [string]$SubscriptionId,

    [Parameter(Mandatory)]
    [ValidatePattern('^[a-zA-Z0-9._\-()]{1,90}$')]
    [string]$ResourceGroup,

    [Parameter(Mandatory)]
    [string]$Location,

    [string[]]$SqlLocation,

    [Parameter(Mandatory)]
    [ValidatePattern('^[a-z0-9]{3,11}$')]
    [string]$NamePrefix,

    [ValidatePattern('^[a-zA-Z][a-zA-Z0-9_]{3,32}$')]
    [string]$SqlAdminUser = 'sqladminuser',

    [securestring]$SqlAdminPassword,

    [string]$DatabaseName = 'shoplogs'
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Get-RedactedCommand {
    # Keeps secrets such as --admin-password out of error messages and transcripts.
    param([Parameter(Mandatory)][string[]]$Arguments)

    $secretFlags = @('--admin-password', '--password', '--client-secret', '--value')
    $safe = [System.Collections.Generic.List[string]]::new()

    for ($i = 0; $i -lt $Arguments.Count; $i++) {
        $safe.Add($Arguments[$i])
        if ($Arguments[$i] -in $secretFlags -and ($i + 1) -lt $Arguments.Count) {
            $safe.Add('***')
            $i++
        }
    }
    return ($safe -join ' ')
}

function Invoke-Az {
    <#
        Runs an az CLI command and throws when it fails, so the script never continues
        on top of a half provisioned environment. Warnings that az writes to stderr are
        kept out of the return value, which is always a single string.
    #>
    param([Parameter(Mandatory)][string[]]$Arguments)

    $merged = az @Arguments 2>&1
    $stdErr = @($merged | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] }) -join "`n"
    $stdOut = @($merged | Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] }) -join "`n"

    if ($LASTEXITCODE -ne 0) {
        throw "az $(Get-RedactedCommand -Arguments $Arguments) failed:`n$stdErr`n$stdOut"
    }
    return $stdOut
}

function Grant-Role {
    <#
        Creates a role assignment and tolerates the "already exists" error so the script
        stays rerunnable.
    #>
    param(
        [Parameter(Mandatory)][string]$ObjectId,
        [Parameter(Mandatory)][ValidateSet('User', 'ServicePrincipal', 'Group')][string]$PrincipalType,
        [Parameter(Mandatory)][string]$Role,
        [Parameter(Mandatory)][string]$Scope
    )

    try {
        Invoke-Az @(
            'role', 'assignment', 'create',
            '--assignee-object-id', $ObjectId,
            '--assignee-principal-type', $PrincipalType,
            '--role', $Role,
            '--scope', $Scope,
            '-o', 'none'
        ) | Out-Null
        Write-Host "    granted '$Role' to $ObjectId"
    }
    catch {
        if ("$_" -match 'RoleAssignmentExists|already exists') {
            Write-Host "    '$Role' was already granted to $ObjectId"
        }
        else {
            throw
        }
    }
}

# --- Prerequisites ----------------------------------------------------------

Write-Step 'Checking prerequisites'

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'Azure CLI (az) was not found. Install it from https://learn.microsoft.com/cli/azure/install-azure-cli'
}

$account = az account show -o json 2>$null | ConvertFrom-Json
if (-not $account) {
    throw 'You are not signed in. Run "az login" first.'
}

if ($SubscriptionId) {
    Invoke-Az @('account', 'set', '--subscription', $SubscriptionId) | Out-Null
    $account = az account show -o json | ConvertFrom-Json
}
$SubscriptionId = $account.id
Write-Host "    Subscription: $($account.name) ($SubscriptionId)"

$extensions = @(az extension list --query "[].name" -o tsv)
if ($extensions -notcontains 'databricks') {
    Write-Host '    Installing the "databricks" CLI extension'
    Invoke-Az @('extension', 'add', '--name', 'databricks', '--only-show-errors') | Out-Null
}

foreach ($provider in 'Microsoft.Sql', 'Microsoft.Databricks', 'Microsoft.Storage') {
    $state = (Invoke-Az @('provider', 'show', '--namespace', $provider, '--query', 'registrationState', '-o', 'tsv')).Trim()
    if ($state -ne 'Registered') {
        Write-Host "    Registering resource provider $provider (currently: $state)"
        Invoke-Az @('provider', 'register', '--namespace', $provider, '--wait') | Out-Null
    }
}

if (-not $SqlAdminPassword) {
    $SqlAdminPassword = Read-Host -Prompt 'SQL administrator password (min. 12 chars, upper + lower + digit + symbol)' -AsSecureString
}
$sqlAdminPasswordPlain = ConvertFrom-SecureString -SecureString $SqlAdminPassword -AsPlainText
if ($sqlAdminPasswordPlain.Length -lt 12) {
    throw 'The SQL administrator password must be at least 12 characters long.'
}

# --- Names ------------------------------------------------------------------
# Storage account names must be globally unique, lowercase alphanumeric and <= 24 characters.

$uniqueSuffix = (
    [System.BitConverter]::ToString(
        [System.Security.Cryptography.SHA256]::HashData(
            [System.Text.Encoding]::UTF8.GetBytes("$SubscriptionId/$ResourceGroup")
        )
    ) -replace '-', ''
).Substring(0, 6).ToLowerInvariant()

$storageAccount = "st$NamePrefix$uniqueSuffix"
$workspaceName = "dbw-$NamePrefix-$uniqueSuffix"
$connectorName = "ac-$NamePrefix-$uniqueSuffix"
$sqlServerBaseName = "sql-$NamePrefix-$uniqueSuffix"

# Azure regularly refuses new SQL servers in busy regions, so keep a fallback list.
$sqlCandidates = if ($SqlLocation) {
    @($SqlLocation)
}
else {
    @($Location, 'swedencentral', 'polandcentral', 'germanywestcentral', 'uksouth',
        'francecentral', 'italynorth', 'eastus2', 'centralus', 'canadacentral') | Select-Object -Unique
}

Write-Host "    Storage account : $storageAccount"
Write-Host "    Databricks      : $workspaceName"
Write-Host "    SQL server      : $sqlServerBaseName-<region>"
Write-Host "    SQL regions     : $($sqlCandidates -join ', ')"

# --- Resource group ---------------------------------------------------------

Write-Step "Creating resource group '$ResourceGroup'"
Invoke-Az @('group', 'create', '--name', $ResourceGroup, '--location', $Location, '-o', 'none') | Out-Null

# --- Storage account (ADLS Gen2) -------------------------------------------

Write-Step "Creating ADLS Gen2 storage account '$storageAccount'"
Invoke-Az @(
    'storage', 'account', 'create',
    '--name', $storageAccount,
    '--resource-group', $ResourceGroup,
    '--location', $Location,
    '--sku', 'Standard_LRS',
    '--kind', 'StorageV2',
    '--hierarchical-namespace', 'true',
    '--min-tls-version', 'TLS1_2',
    '--allow-blob-public-access', 'false',
    '-o', 'none'
) | Out-Null

$storageId = (Invoke-Az @('storage', 'account', 'show', '--name', $storageAccount, '--resource-group', $ResourceGroup, '--query', 'id', '-o', 'tsv')).Trim()

Write-Step 'Granting the signed-in user "Storage Blob Data Contributor" on the storage account'
$currentUserObjectId = (Invoke-Az @('ad', 'signed-in-user', 'show', '--query', 'id', '-o', 'tsv')).Trim()
Grant-Role -ObjectId $currentUserObjectId -PrincipalType User -Role 'Storage Blob Data Contributor' -Scope $storageId

Write-Step 'Creating the "raw" and "processed" containers'
foreach ($container in 'raw', 'processed') {
    $attempt = 0
    while ($true) {
        $attempt++
        try {
            Invoke-Az @(
                'storage', 'fs', 'create',
                '--name', $container,
                '--account-name', $storageAccount,
                '--auth-mode', 'login',
                '-o', 'none'
            ) | Out-Null
            Write-Host "    container '$container' created"
            break
        }
        catch {
            if ("$_" -match 'ContainerAlreadyExists|FilesystemAlreadyExists') {
                Write-Host "    container '$container' already exists"
                break
            }
            # Role assignments are eventually consistent, so retry only while RBAC propagates.
            if ("$_" -match 'AuthorizationPermissionMismatch|AuthorizationFailure|AuthenticationFailed' -and $attempt -lt 10) {
                Write-Host "    waiting for RBAC propagation ($attempt/10)..."
                Start-Sleep -Seconds 15
                continue
            }
            throw
        }
    }
}

# --- Databricks workspace ---------------------------------------------------
# Premium is required for Unity Catalog, secret scope ACLs and credential passthrough.
# "create" is not idempotent: it generates a new random managed resource group name every
# time, which collides with the existing one, so check for the workspace first.

$workspaceUrl = az databricks workspace show --resource-group $ResourceGroup --name $workspaceName --query workspaceUrl -o tsv 2>$null

if ($LASTEXITCODE -eq 0 -and $workspaceUrl) {
    $workspaceUrl = $workspaceUrl.Trim()
    Write-Step "Databricks workspace '$workspaceName' already exists"
}
else {
    Write-Step "Creating Databricks workspace '$workspaceName' (Premium SKU)"
    Invoke-Az @(
        'databricks', 'workspace', 'create',
        '--resource-group', $ResourceGroup,
        '--name', $workspaceName,
        '--location', $Location,
        '--sku', 'premium',
        '-o', 'none'
    ) | Out-Null

    $workspaceUrl = (Invoke-Az @('databricks', 'workspace', 'show', '--resource-group', $ResourceGroup, '--name', $workspaceName, '--query', 'workspaceUrl', '-o', 'tsv')).Trim()
}

$connectorId = az databricks access-connector show --resource-group $ResourceGroup --name $connectorName --query id -o tsv 2>$null

if ($LASTEXITCODE -eq 0 -and $connectorId) {
    $connectorId = $connectorId.Trim()
    Write-Step "Databricks Access Connector '$connectorName' already exists"
}
else {
    Write-Step "Creating Databricks Access Connector '$connectorName' for keyless storage access"
    Invoke-Az @(
        'databricks', 'access-connector', 'create',
        '--resource-group', $ResourceGroup,
        '--name', $connectorName,
        '--location', $Location,
        '--identity-type', 'SystemAssigned',
        '-o', 'none'
    ) | Out-Null

    $connectorId = (Invoke-Az @('databricks', 'access-connector', 'show', '--resource-group', $ResourceGroup, '--name', $connectorName, '--query', 'id', '-o', 'tsv')).Trim()
}

$connectorPrincipalId = (Invoke-Az @('databricks', 'access-connector', 'show', '--resource-group', $ResourceGroup, '--name', $connectorName, '--query', 'identity.principalId', '-o', 'tsv')).Trim()

Grant-Role -ObjectId $connectorPrincipalId -PrincipalType ServicePrincipal -Role 'Storage Blob Data Contributor' -Scope $storageId

# --- Azure SQL --------------------------------------------------------------
# A failed "sql server create" still binds the name to the region it was attempted in, even
# though no resource becomes visible. Retrying the same name elsewhere then fails forever with
# InvalidResourceLocation, so every region candidate gets its own server name.

$existingSql = az sql server list --resource-group $ResourceGroup --query "[?starts_with(name, '$sqlServerBaseName')] | [0].{name:name, location:location}" -o json 2>$null | ConvertFrom-Json

if ($existingSql) {
    $sqlServerName = $existingSql.name
    $sqlLocationUsed = $existingSql.location
    Write-Step "Azure SQL server '$sqlServerName' already exists in $sqlLocationUsed"
}
else {
    $sqlServerName = $null
    $sqlLocationUsed = $null

    foreach ($candidate in $sqlCandidates) {
        $attemptName = "$sqlServerBaseName-$candidate"
        Write-Step "Creating Azure SQL server '$attemptName' in $candidate"
        try {
            Invoke-Az @(
                'sql', 'server', 'create',
                '--name', $attemptName,
                '--resource-group', $ResourceGroup,
                '--location', $candidate,
                '--admin-user', $SqlAdminUser,
                '--admin-password', $sqlAdminPasswordPlain,
                '--minimal-tls-version', '1.2',
                '-o', 'none'
            ) | Out-Null
            $sqlServerName = $attemptName
            $sqlLocationUsed = $candidate
            break
        }
        catch {
            $azError = "$_"
            if ($azError -match 'RegionDoesNotAllowProvisioning|ProvisioningDisabled|LocationNotAvailable|NotAvailableForSubscription|InvalidResourceLocation') {
                Write-Warning "$candidate is not accepting new SQL servers, trying the next region"
                continue
            }
            throw
        }
    }

    if (-not $sqlServerName) {
        throw "None of these regions accepted a new Azure SQL server: $($sqlCandidates -join ', '). Pass -SqlLocation with other regions."
    }
}

Write-Step "Creating database '$DatabaseName' (serverless General Purpose, auto-pause after 60 min)"
Invoke-Az @(
    'sql', 'db', 'create',
    '--name', $DatabaseName,
    '--server', $sqlServerName,
    '--resource-group', $ResourceGroup,
    '--edition', 'GeneralPurpose',
    '--family', 'Gen5',
    '--capacity', '1',
    '--compute-model', 'Serverless',
    '--auto-pause-delay', '60',
    '--backup-storage-redundancy', 'Local',
    '-o', 'none'
) | Out-Null

Write-Step 'Configuring SQL firewall rules'
# Required so Databricks (and other Azure services) can reach the database.
Invoke-Az @(
    'sql', 'server', 'firewall-rule', 'create',
    '--name', 'AllowAzureServices',
    '--server', $sqlServerName,
    '--resource-group', $ResourceGroup,
    '--start-ip-address', '0.0.0.0',
    '--end-ip-address', '0.0.0.0',
    '-o', 'none'
) | Out-Null

$clientIp = try {
    (Invoke-RestMethod -Uri 'https://api.ipify.org?format=json' -TimeoutSec 15).ip
}
catch {
    $null
}

if ($clientIp) {
    $clientIp = $clientIp.Trim()
    Invoke-Az @(
        'sql', 'server', 'firewall-rule', 'create',
        '--name', 'AllowClientIp',
        '--server', $sqlServerName,
        '--resource-group', $ResourceGroup,
        '--start-ip-address', $clientIp,
        '--end-ip-address', $clientIp,
        '-o', 'none'
    ) | Out-Null
    Write-Host "    allowed client IP $clientIp"
}
else {
    Write-Warning 'Could not detect your public IP. Add a SQL firewall rule manually before seeding data.'
}

# --- Summary ----------------------------------------------------------------

$outputs = [ordered]@{
    subscriptionId          = $SubscriptionId
    resourceGroup           = $ResourceGroup
    location                = $Location
    storageAccount          = $storageAccount
    rawPath                 = "abfss://raw@$storageAccount.dfs.core.windows.net/"
    processedPath           = "abfss://processed@$storageAccount.dfs.core.windows.net/"
    databricksWorkspace     = $workspaceName
    databricksHost          = "https://$workspaceUrl"
    accessConnectorId       = $connectorId
    sqlLocation             = $sqlLocationUsed
    sqlServerFqdn           = "$sqlServerName.database.windows.net"
    sqlDatabase             = $DatabaseName
    sqlAdminUser            = $SqlAdminUser
}

$outputFile = Join-Path $PSScriptRoot '.azure-outputs.json'
$outputs | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $outputFile -Encoding utf8

Write-Step 'Environment ready'
$outputs.GetEnumerator() | ForEach-Object { '    {0,-20} {1}' -f $_.Key, $_.Value }

Write-Host @"

Saved to $outputFile (git-ignored). The SQL password is intentionally NOT stored.

Next steps:
  1. Create the log tables (task 02). The database is empty on purpose:
       Azure Portal -> SQL databases -> $DatabaseName -> Query editor (preview)
       Sign in as '$SqlAdminUser', then run sql/01_create_log_schema.sql
       followed by sql/02_insert_deterministic_test_data.sql.
       Neither script uses GO, so each can be pasted and run in one go.
  2. databricks configure --host $($outputs.databricksHost)
  3. In the Databricks account console, create a metastore and assign it to the workspace.
  4. Create a storage credential from the access connector:
       $connectorId
  5. Create an external location pointing at $($outputs.rawPath)
"@ -ForegroundColor Green

