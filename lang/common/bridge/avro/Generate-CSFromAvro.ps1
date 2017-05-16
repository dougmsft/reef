
#
# This script is a temporary means of hand generating the C# source files
# for the Java to CLR Avro messages.

$SourcePath = [Environment]::GetEnvironmentVariable("REEFSourcePath")
if ($SourcePath -eq $null)
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
    "CLRInitialized.avsc",
    "SystemOnStart.avsc"
)


foreach ($SourceFile in $AvroSources)
{
    & "Microsoft.Avro.Tools.exe" CodeGen /I:$SourceFile /O:$SourcePath\lang\cs\Org.Apache.REEF.Bridge.CLR\Message\
}


