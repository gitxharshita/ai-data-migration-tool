# Builds the report, renders it with Word, fills in page numbers, renders again.
#   powershell -ExecutionPolicy Bypass -File report\render_report.ps1
param([string]$Template = "$env:USERPROFILE\Downloads\Project_Report_Format_IITJ.docx")
$ErrorActionPreference = "Stop"
$here = $PSScriptRoot
$name = "Harshita_Gupta_M25DE2016_MTP_Report"
$tmp = "$env:TEMP\amt_report"
New-Item -ItemType Directory -Force $tmp | Out-Null

function Render([string]$docx, [string]$pdf) {
    # Word refuses very long OneDrive paths, so work on a copy in the temp folder
    Copy-Item $docx "$tmp\r.docx" -Force
    $w = New-Object -ComObject Word.Application
    $w.Visible = $false
    try {
        $d = $w.Documents.Open("$tmp\r.docx", $false, $true)
        $d.ExportAsFixedFormat("$tmp\r.pdf", 17)
        $d.Close($false)
    } finally { $w.Quit() }
    Copy-Item "$tmp\r.pdf" $pdf -Force
}

python "$here\build_report.py" $Template "$here\$name.docx"
Render "$here\$name.docx" "$here\$name.pdf"
python "$here\find_pages.py" "$here\$name.pdf" "$tmp\pages1.json" "$here\build_report.py"
for ($round = 2; $round -le 3; $round++) {
    python "$here\build_report.py" $Template "$here\$name.docx" "$tmp\pages$($round - 1).json"
    Render "$here\$name.docx" "$here\$name.pdf"
    python "$here\find_pages.py" "$here\$name.pdf" "$tmp\pages$round.json" "$here\build_report.py"
    if ((Get-Content "$tmp\pages$round.json" -Raw) -eq (Get-Content "$tmp\pages$($round - 1).json" -Raw)) {
        "page numbers stable after round $round"; break
    }
}
