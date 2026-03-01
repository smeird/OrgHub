#!/bin/zsh
cd /Users/dominicbundy/AutomationHub/email-filing
source .venv/bin/activate
export EMAIL_ATTACHMENTS_BASE=/Users/dominicbundy/AutomationHub/email-filing
exec uvicorn web.app:app --host 127.0.0.1 --port 8000
