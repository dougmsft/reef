
$NUGET_EXE = "./nuget.exe"
# Try download NuGet.exe if do not exist.
if (!(Test-Path $NUGET_EXE)) {
    Invoke-WebRequest -Uri http://dist.nuget.org/win-x86-commandline/v3.5.0/NuGet.exe -OutFile $NUGET_EXE
}

# Call nuget to restore the packages
& .\nuget.exe restore .\packages.config -PackagesDirectory .\packages

# Copy the boost libraries to a single directory so that they can be found by cmake
$source=".\packages"
$destination=".\packages\boost.1.64.0.0\lib\native\address-model-64"

$lib_dir_exists = Test-Path $destination
if ($lib_dir_exists) {
    Remove-Item $destination -Force -Recurse
}
New-Item -ItemType directory -Path $destination

$filter = [regex]"[^/]+\\address-model-64\\[^/]+$"
$bin = Get-ChildItem -rec | Where-Object {!$_.PSIsContainer} | ForEach-Object -Process {$_.FullName}
$filtered = $bin -match $filter
foreach ($item in $filtered) {
    Copy-Item -Path $item -Destination $destination
}

# Generate the VS projects
$build_dir=".\build"
if (Test-Path $build_dir) {
    Remove-Item $build_dir -Force -Recurse
}

New-Item -ItemType directory -Path $build_dir
& cd $build_dir;cmake .. -G "Visual Studio 14 2015 Win64"

