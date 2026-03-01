#!/bin/zsh
cd /Users/dominicbundy/Documents/Email-Attachments
source .venv/bin/activate
exec uvicorn web.app:app --host 127.0.0.1 --port 8000
