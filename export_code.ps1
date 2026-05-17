$files = Get-ChildItem -Recurse -Include *.py, *.js, *.css, *.html, *.txt, *.sql, *.md | Where-Object { 
    $_.FullName -notmatch 'node_modules|venv|.git|.pytest_cache|__pycache__|full_system_export.txt|.db|.xlsx|.gz|.png|.jpg|.jpeg' 
}


foreach ($file in $files) {
    Write-Output "========================================"
    Write-Output "FILE: $($file.FullName)"
    Write-Output "========================================"
    Get-Content $file.FullName
    Write-Output "`n`n"
}
