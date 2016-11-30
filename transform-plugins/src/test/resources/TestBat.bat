@echo off
set usr=
:while1
    if [%usr%] neq [""] (
		set usr=""
		set /p usr=""
		setlocal EnableDelayedExpansion
		echo "You entered !usr!"
        goto :while1
    )
endlocal