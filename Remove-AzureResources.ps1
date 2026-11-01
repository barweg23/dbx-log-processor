#Requires -Version 7.0
<#
.SYNOPSIS
    Deletes the resource group created by Create-AzureResources.ps1.

.DESCRIPTION
    Removing the resource group deletes every resource of the challenge, including the
    Databricks managed resource group. Use it as soon as you finish the challenge to stop
    the Azure spend.

.PARAMETER ResourceGroup
    The resource group to delete.

.PARAMETER SubscriptionId
    Target subscription. Defaults to the currently selected one.

.PARAMETER Force
    Deletes without the interactive confirmation prompt.

.PARAMETER Wait
    Blocks until the deletion has finished. Use this before reprovisioning into the same resource
    group, because Azure keeps resource names reserved until the delete completes.

.EXAMPLE
    ./Remove-AzureResources.ps1 -ResourceGroup rg-dbx-logs-demo

.EXAMPLE
    ./Remove-AzureResources.ps1 -ResourceGroup rg-dbx-logs-demo -Force -Wait
#>
[CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
param(
    [Parameter(Mandatory)]
    [string]$ResourceGroup,

    [string]$SubscriptionId,

    [switch]$Force,

    [switch]$Wait
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'Azure CLI (az) was not found.'
}

az account show -o none 2>$null
if ($LASTEXITCODE -ne 0) {
    throw 'You are not signed in. Run "az login" first.'
}

if ($SubscriptionId) {
    az account set --subscription $SubscriptionId
}

$exists = az group exists --name $ResourceGroup -o tsv
if ($exists -ne 'true') {
    Write-Warning "Resource group '$ResourceGroup' does not exist. Nothing to do."
    return
}

Write-Host "The following resources will be deleted from '$ResourceGroup':" -ForegroundColor Yellow
az resource list --resource-group $ResourceGroup --query "[].{name:name, type:type}" -o table

if ($Force -or $PSCmdlet.ShouldProcess($ResourceGroup, 'Delete resource group and all its resources')) {
    $outputFile = Join-Path $PSScriptRoot '.azure-outputs.json'
    if (Test-Path -LiteralPath $outputFile) {
        Remove-Item -LiteralPath $outputFile -Force
    }

    if ($Wait) {
        Write-Host "Deleting '$ResourceGroup'. This blocks until Azure has finished, which can take 10+ minutes because of the Databricks managed resource group." -ForegroundColor Yellow
        az group delete --name $ResourceGroup --yes
        Write-Host "'$ResourceGroup' deleted. Resource names are free to reuse." -ForegroundColor Green
    }
    else {
        az group delete --name $ResourceGroup --yes --no-wait
        Write-Host @"
Deletion started for '$ResourceGroup'. It runs in the background.

Azure keeps resource names reserved until the delete completes, so wait for this before
reprovisioning into the same resource group:
    az group exists --name $ResourceGroup     # wait until it prints false
"@ -ForegroundColor Green
    }
}

