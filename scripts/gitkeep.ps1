$base = "d:\Financial Project\Macro Data Dashboard PRD"
$dirs = @("docker\airflow\logs", "docker\airflow\plugins", "backend\app\calculators", "frontend\src\components", "frontend\public", "scripts")

foreach ($d in $dirs) {
    $f = Join-Path $base "$d\.gitkeep"
    New-Item -ItemType File -Path $f -Force | Out-Null
}
Write-Host "Done"
