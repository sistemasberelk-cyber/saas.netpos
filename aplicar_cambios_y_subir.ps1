Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "   NEXPOS SAAS - ACTUALIZACION Y DEPLOY        " -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "1. Cifrando claves API (Fix 7)..." -ForegroundColor Yellow
python scripts\backfill_stock_to_wms.py --encrypt-keys

Write-Host ""
Write-Host "2. Migrando stock a WMS (Fix 4)..." -ForegroundColor Yellow
python scripts\backfill_stock_to_wms.py

Write-Host ""
Write-Host "3. Aplicando migraciones de base de datos (Alembic)..." -ForegroundColor Yellow
alembic upgrade head

Write-Host ""
Write-Host "4. Subiendo cambios a GitHub..." -ForegroundColor Yellow
git add .
git commit -m "Aplicados fixes de Claude: WMS, cuenta corriente, refactor de modelos y dashboard"
git push

Write-Host ""
Write-Host "===============================================" -ForegroundColor Green
Write-Host " ¡PROCESO COMPLETADO EXITOSAMENTE!             " -ForegroundColor Green
Write-Host "===============================================" -ForegroundColor Green
pause
