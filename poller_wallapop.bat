@echo off
REM --- Archivo: poller_wallapop.bat ---

REM Cambiar al directorio donde está el script
cd /d "C:\Users\jonat\Desktop\GR_2\Wallapop"

REM Ruta del intérprete de Python
set PYTHON_EXE="C:\Users\jonat\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\python.exe"

REM Nombre del script Python
set SCRIPT="poller_wallapop.py"

REM Archivo log (guardará salida y errores)
set LOG="poller_wallapop_log.txt"

REM Ejecutar el script y guardar la salida en el log (agrega >> para añadir)
%PYTHON_EXE% %SCRIPT% >> %LOG% 2>&1
