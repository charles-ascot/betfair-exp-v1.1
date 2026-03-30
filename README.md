# Betfair Historic Data Explorer

A web application for browsing and downloading historical market data from the Betfair Exchange Historical Data API. Features real-time download progress, live log streaming, and Google Cloud Storage integration.

## Tech Stack

- **Frontend:** React 18 + Vite — hosted on GitHub Pages
- **Backend:** FastAPI + Uvicorn — deployed on Google Cloud Run
- **Storage:** Google Cloud Storage (GCS)
- **Container:** Docker (Python 3.11-slim)

## Features

- Betfair SSO authentication
- Date range and data tier selection (Basic / Advanced / Pro)
- Country filtering (GB & IE) and market type filtering
- File availability preview with count and size estimates
- Streaming downloads with real-time progress tracking (SSE)
- Live Cloud Run log panel
- Automatic upload to GCS

## Project Structure

```
betfair1/
├── frontend/              # React + Vite app
│   ├── src/
│   │   ├── App.jsx        # Main UI component
│   │   └── App.css        # Styles
│   └── package.json
├── backend-local/         # FastAPI backend
│   ├── main.py            # API endpoints & download logic
│   └── requirements.txt
├── Dockerfile             # Cloud Run container build
└── project-docs/          # API documentation
```

## Development

### Frontend

```bash
cd frontend
npm install
npm run dev
```

### Backend

```bash
cd backend-local
pip install -r requirements.txt
uvicorn main:app --reload
```

## Deployment

- **Frontend** auto-deploys to GitHub Pages on push to `main`
- **Backend** auto-deploys to Google Cloud Run on push to `main`

## Changelog

### 2026-03-25
- Fixed 500 error on `getAdvBasketDataSize` and `GetCollectionOptions` endpoints — Betfair's Historical Data API throws a .NET Runtime Error when `null` is sent for `eventId`/`eventName` fields. These fields are now omitted from the request body unless they carry an actual value.
