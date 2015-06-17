<#
.SYNOPSIS
    Powershell script to build Riak Go Client on Windows
.DESCRIPTION
    This script ensures that your build environment is sane and will run 'go' correctly depending on parameters passed to this script.
.PARAMETER Target
    Target to build. Can be one of the following:
        * ProtoGen - generate *.pb.go files from *.proto files. Requires Go protoc utility (https://github.com/golang/protobuf)
        * Format   - run *.go files through 'go fmt'
.PARAMETER Verbose
    Use to increase verbosity.
.EXAMPLE
    C:\Users\Bashoman> cd go\src\github.com\basho\riak-go-client
    C:\Users\Bashoman\go\src\github.com\basho\riak-go-client>.\make.ps1 -Target Protoc -Verbose
.NOTES
    Author: Luke Bakken
    Date:   June 1, 2015
#>
[CmdletBinding()]
Param(
    [Parameter(Mandatory=$False, Position=0)]
    [ValidateSet('ProtoGen', 'Format', IgnoreCase = $True)]
    [string]$Target = 'Format'
)

Set-StrictMode -Version Latest

$IsDebug = $DebugPreference -ne 'SilentlyContinue'
$IsVerbose = $VerbosePreference -ne 'SilentlyContinue'

# Note:
# Set to Continue to see DEBUG messages
if ($IsVerbose) {
    $DebugPreference = 'Continue'
}

trap
{
    Write-Error -ErrorRecord $_
    exit 1
}

function Get-ScriptPath {
    $scriptDir = Get-Variable PSScriptRoot -ErrorAction SilentlyContinue | ForEach-Object { $_.Value }
    if (!$scriptDir) {
        if ($MyInvocation.MyCommand.Path) {
            $scriptDir = Split-Path $MyInvocation.MyCommand.Path -Parent
        }
    }
    if (!$scriptDir) {
        if ($ExecutionContext.SessionState.Module.Path) {
            $scriptDir = Split-Path (Split-Path $ExecutionContext.SessionState.Module.Path)
        }
    }
    if (!$scriptDir) {
        $scriptDir = $PWD
    }
    return $scriptDir
}

function Do-ProtoGen {
    $script_path = Get-ScriptPath
    $rpb_path = Join-Path -Path $script_path -ChildPath 'rpb'
    $proto_path = Join-Path -Path $script_path -ChildPath (Join-Path -Path 'riak_pb' -ChildPath 'src')
    $proto_wild = Join-Path -Path $proto_path -ChildPath '*.proto'
    Get-ChildItem $proto_wild | ForEach-Object {
        $rpb_path_tmp = Join-Path -Path $rpb_path -ChildPath $_.BaseName
        If (!(Test-Path $rpb_path_tmp)) {
            New-Item $rpb_path_tmp -Type Directory -Force
        }
        Write-Verbose "protoc: --go_out=$rpb_path_tmp --proto_path=$proto_path $_"
        & { protoc --go_out=$rpb_path_tmp --proto_path=$proto_path $_ }
        if ($? -ne $True) {
            throw "protoc.exe failed: $LastExitCode"
        }

        $base_name = $_.BaseName
        $rpb_file = Join-Path -Path $rpb_path_tmp -ChildPath "$base_name.pb.go"
        Write-Verbose "post-processing $rpb_file"

        (Get-Content $rpb_file) |
            ForEach-Object {
                $_ -Replace 'import proto "code.google.com/p/goprotobuf/proto"', 'import proto "github.com/golang/protobuf/proto"'
            } | Set-Content $rpb_file

        if ($_.Name -eq 'riak_search.proto' -or $_.Name -eq 'riak_kv.proto') {
            (Get-Content $rpb_file) |
                ForEach-Object {
                    $_ -Replace 'import riak "riak.pb"', 'import riak "github.com/basho-labs/riak-go-client/rpb/riak"'
                } | Set-Content $rpb_file
        }
    }
}

function Do-Format {
    $script_path = Get-ScriptPath
    Write-Verbose "go fmt $script_path"
    & { gofmt -s -w $script_path }
}

Write-Debug "Target: $Target"

switch ($Target)
{
    'ProtoGen' { Do-ProtoGen }
    'Format' { Do-Format }
     default { throw "Unknown target: $Target" }
}

exit 0
