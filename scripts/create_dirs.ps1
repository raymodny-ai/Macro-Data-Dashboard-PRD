$base = "d:\Financial Project\Macro Data Dashboard PRD"
$dirs = @(
    "docker\airflow",
    "db\init",
    "backend\app\routers",
    "backend\app\calculators",
    "celery-worker\app",
    "frontend\src\components",
    "frontend\src\stores",
    "frontend\src\services",
    "frontend\public",
    "scripts"
)

foreach ($d in $dirs) {
    $path = Join-Path $base $d
    New-Item -ItemType Directory -Path $path -Force | Out-Null
    Write-Host "Created: $path"
}
