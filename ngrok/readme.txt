Terminal as admin

cd to this directory

nssm.exe install ngrok-service-name-of-your-choice

point to ngrok.exe
path will fill in automatically

add: argument

start --all --config="C:\path\to\my\ngrok.yml"

sc start ngrok-service-name-of-your-choice