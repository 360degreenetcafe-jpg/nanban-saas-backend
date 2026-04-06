# Pushes secret values that exist in this repo (gode.gs + index.html) to Secret Manager.
# Requires: gcloud auth logged in, project nanban-driving-school-d7b20
$ErrorActionPreference = "Stop"
$Project = "nanban-driving-school-d7b20"
$Gcloud = "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
if (-not (Test-Path $Gcloud)) { $Gcloud = "gcloud" }

$Root = Split-Path $PSScriptRoot -Parent
if (-not (Test-Path (Join-Path $Root "gode.gs"))) { $Root = (Get-Location).Path }

$gode = Join-Path $Root "gode.gs"
$html = Join-Path $Root "index.html"
if (-not (Test-Path $gode)) { throw "gode.gs not found at $gode" }

$raw = Get-Content $gode -Raw -Encoding UTF8
if ($raw -notmatch 'const WA_TOKEN = "([^"]+)"') { throw "Could not parse WA_TOKEN from gode.gs" }
$waToken = $Matches[1]
if ($raw -notmatch 'const WA_PHONE_ID = "([^"]+)"') { throw "Could not parse WA_PHONE_ID from gode.gs" }
$waPhone = $Matches[1]

$bridgeUrl = $null
if (Test-Path $html) {
  $htmlRaw = Get-Content $html -Raw -Encoding UTF8
  if ($htmlRaw -match 'DEFAULT_BRIDGE_ENDPOINT\s*=\s*"([^"]+)"') { $bridgeUrl = $Matches[1] }
}

function Add-SecretVersion($name, $value) {
  $tmp = Join-Path $env:TEMP ("gsec_" + [Guid]::NewGuid().ToString("N") + ".txt")
  $enc = New-Object System.Text.UTF8Encoding $false
  [System.IO.File]::WriteAllText($tmp, $value, $enc)
  try {
    & $Gcloud secrets versions add $name --data-file=$tmp --project=$Project
    if ($LASTEXITCODE -ne 0) { throw "gcloud failed for $name" }
    Write-Host "OK: $name"
  } finally {
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
  }
}

Write-Host "Adding WHATSAPP_GRAPH_TOKEN, WHATSAPP_PHONE_NUMBER_ID from gode.gs ..."
Add-SecretVersion "WHATSAPP_GRAPH_TOKEN" $waToken
Add-SecretVersion "WHATSAPP_PHONE_NUMBER_ID" $waPhone

if ($bridgeUrl) {
  Write-Host "Adding LEGACY_GAS_BRIDGE_URL from index.html ..."
  Add-SecretVersion "LEGACY_GAS_BRIDGE_URL" $bridgeUrl
} else {
  Write-Host "SKIP: LEGACY_GAS_BRIDGE_URL (index.html DEFAULT_BRIDGE_ENDPOINT not found)"
}

Write-Host ""
Write-Host "Done with repo-sourced secrets."
Write-Host "You must still add manually (Meta / Apps Script / your choice):"
Write-Host "  - WHATSAPP_VERIFY_TOKEN"
Write-Host "  - WHATSAPP_APP_SECRET"
Write-Host "  - LEGACY_GAS_BRIDGE_KEY  (Script Properties WEB_BRIDGE_KEY in Apps Script)"
Write-Host "  - INTERNAL_OPS_KEY       (pick a strong random string for x-ops-key)"
