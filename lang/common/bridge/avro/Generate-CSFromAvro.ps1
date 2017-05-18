
#
# Temporarily hand generate the C# source files for the Java to CLR bridge 
# Avro messages until this is fully supported on the C# side.
#

$ReefRoot = [Environment]::GetEnvironmentVariable("REEFSourcePath")
if ($ReefRoot -eq $null)
{
    Write-Host "REEFSourcePath must be set" -ForegroundColor Red
    exit
}

if ((Get-Command "Microsoft.Avro.Tools.exe" -ErrorAction SilentlyContinue) -eq $null)
{
    Write-Host "Microsoft.Avro.Tools.exe must be in path" -ForegroundColor Red
    exit
}

$AvroSources = @(
    "Header.avsc",
    "Protocol.avsc",
    "SystemOnStart.avsc"
)

foreach ($SourceFile in $AvroSources)
{
    Write-Host "Processing $SourceFile" -ForegroundColor Green
    & "Microsoft.Avro.Tools.exe" CodeGen /I:$ReefRoot\lang\common\bridge\avro\$SourceFile /O:$ReefRoot\lang\cs\Org.Apache.REEF.Bridge.CLR\Message\
}


