$ErrorActionPreference = 'Stop'

Set-Location (Join-Path $PSScriptRoot '..')

Write-Host 'Building base image...'
docker build -t ngs/base-agent:latest -f .\agents\base\Dockerfile .

$agents = @('ingest', 'qc', 'trim', 'align', 'count', 'de')
foreach ($agent in $agents) {
    Write-Host "Building $agent agent..."
    docker build -t "ngs/$agent-agent:latest" ".\agents\$agent"
}

Write-Host 'All agents built successfully.'
