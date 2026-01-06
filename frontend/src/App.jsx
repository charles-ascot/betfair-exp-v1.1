import React, { useState, useRef, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const API_BASE = 'https://betfair-backend-1026419041222.europe-west2.run.app';

const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const YEARS = Array.from({ length: 10 }, (_, i) => 2024 - i);

export default function App() {
  const [introComplete, setIntroComplete] = useState(false);
  const [ssoid, setSsoid] = useState('');
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  // Simplified form state - only month and year
  const [fromMonth, setFromMonth] = useState('1');
  const [fromYear, setFromYear] = useState('2024');
  const [toMonth, setToMonth] = useState('12');
  const [toYear, setToYear] = useState('2024');

  // Data tier selection
  const [selectedPlan, setSelectedPlan] = useState('Basic Plan');
  const DATA_TIERS = [
    { value: 'Basic Plan', label: 'Basic Plan', description: 'Standard market data' },
    { value: 'Advanced Plan', label: 'Advanced Plan', description: 'Detailed tick-level data' },
    { value: 'Pro Plan', label: 'Pro Plan', description: 'Full depth market data' }
  ];

  // Pre-selected defaults (hardcoded)
  const selectedMarkets = []; // All market types
  const selectedCountries = ['GB']; // Only GB
  const selectedFileTypes = ['M', 'E']; // Both M and E

  const [fileCount, setFileCount] = useState(0);
  const [totalSizeMB, setTotalSizeMB] = useState(0);
  const [hasChecked, setHasChecked] = useState(false);

  // New streaming download state
  const [isDownloading, setIsDownloading] = useState(false);
  const [downloadProgress, setDownloadProgress] = useState(null);
  const [currentJobId, setCurrentJobId] = useState(null);
  const [failedFilesCount, setFailedFilesCount] = useState(0);
  const [downloadComplete, setDownloadComplete] = useState(false);
  const eventSourceRef = useRef(null);

  // Clean up EventSource on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    };
  }, []);

  // Clear results when date range changes
  const handleDateChange = (setter) => (e) => {
    setter(e.target.value);
    setHasChecked(false);
    setFileCount(0);
    setTotalSizeMB(0);
    setDownloadProgress(null);
    setCurrentJobId(null);
    setFailedFilesCount(0);
    setDownloadComplete(false);
  };

  const handleConnect = async () => {
    if (!ssoid.trim()) {
      setError('Please enter your ssoid token');
      return;
    }

    setLoading(true);
    setError('');
    setSuccess('');

    try {
      const response = await axios.post(`${API_BASE}/api/GetMyData`, { ssoid });
      if (response.status === 200) {
        setIsLoggedIn(true);
        setSuccess('Successfully authenticated');
        setTimeout(() => setSuccess(''), 3000);
      }
    } catch (err) {
      setError('Failed to authenticate. Check your ssoid token.');
    } finally {
      setLoading(false);
    }
  };

  const handleCheckAvailability = async () => {
    setLoading(true);
    setError('');
    setSuccess('');

    try {
      const response = await axios.post(`${API_BASE}/api/getAdvBasketDataSize`, {
        ssoid,
        sport: 'Horse Racing',
        plan: selectedPlan,
        fromDay: 1,
        fromMonth: parseInt(fromMonth),
        fromYear: parseInt(fromYear),
        toDay: 31,
        toMonth: parseInt(toMonth),
        toYear: parseInt(toYear),
        marketTypesCollection: selectedMarkets,
        countriesCollection: selectedCountries,
        fileTypeCollection: selectedFileTypes
      });

      setFileCount(response.data.fileCount);
      setTotalSizeMB(response.data.totalSizeMB);
      setHasChecked(true);
      setDownloadComplete(false);
      setDownloadProgress(null);
      setSuccess(`Found ${response.data.fileCount} files, ${response.data.totalSizeMB} MB total`);
      setTimeout(() => setSuccess(''), 4000);
    } catch (err) {
      setError('Failed to check availability. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleStartDownload = async () => {
    setError('');
    setSuccess('');
    setIsDownloading(true);
    setDownloadComplete(false);
    setFailedFilesCount(0);

    try {
      // Step 1: Get list of files
      setSuccess('Fetching file list from Betfair...');
      const listResponse = await axios.post(`${API_BASE}/api/downloadListOfFiles`, {
        ssoid,
        sport: 'Horse Racing',
        plan: selectedPlan,
        fromDay: 1,
        fromMonth: parseInt(fromMonth),
        fromYear: parseInt(fromYear),
        toDay: 31,
        toMonth: parseInt(toMonth),
        toYear: parseInt(toYear),
        marketTypesCollection: selectedMarkets,
        countriesCollection: selectedCountries,
        fileTypeCollection: selectedFileTypes
      });

      const filePaths = listResponse.data;
      if (!filePaths || filePaths.length === 0) {
        setError('No files found to download');
        setIsDownloading(false);
        return;
      }

      // Step 2: Start download job with request parameters
      setSuccess('Starting download job...');
      const jobResponse = await axios.post(`${API_BASE}/api/startDownloadJob`, {
        ssoid,
        filePaths,
        // Include request parameters for caching/verification
        plan: selectedPlan,
        sport: 'Horse Racing',
        fromMonth: parseInt(fromMonth),
        fromYear: parseInt(fromYear),
        toMonth: parseInt(toMonth),
        toYear: parseInt(toYear),
        countriesCollection: selectedCountries,
        fileTypeCollection: selectedFileTypes,
        marketTypesCollection: selectedMarkets
      });

      const jobId = jobResponse.data.jobId;
      setCurrentJobId(jobId);

      // Step 3: Connect to SSE stream for progress
      setSuccess('Connecting to download stream...');

      const eventSource = new EventSource(`${API_BASE}/api/streamDownload/${jobId}`);
      eventSourceRef.current = eventSource;

      eventSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          switch (data.type) {
            case 'start':
              setSuccess(`Download started: ${data.totalFiles} files`);
              setDownloadProgress({
                completed: 0,
                failed: 0,
                skipped: 0,
                total: data.totalFiles,
                percent: 0,
                currentFile: null,
                requestParams: data.requestParams
              });
              break;

            case 'progress':
              setDownloadProgress({
                completed: data.completed,
                failed: data.failed,
                skipped: data.skipped || 0,
                total: data.total,
                percent: data.percent,
                currentFile: data.currentFile
              });
              setFailedFilesCount(data.failed);
              break;

            case 'complete':
              setDownloadProgress({
                completed: data.completed,
                failed: data.failed,
                skipped: data.skipped || 0,
                total: data.total,
                percent: 100,
                currentFile: null,
                bucket: data.bucket
              });
              setDownloadComplete(true);
              setIsDownloading(false);
              const skippedMsg = data.skipped > 0 ? ` (${data.skipped} already existed)` : '';
              setSuccess(`Download complete! ${data.completed} files uploaded to gs://${data.bucket}/${skippedMsg}`);
              eventSource.close();
              eventSourceRef.current = null;
              break;

            case 'aborted':
              setDownloadProgress(prev => ({
                ...prev,
                completed: data.completed,
                failed: data.failed,
                skipped: data.skipped || 0
              }));
              setIsDownloading(false);
              setSuccess(`Download aborted. ${data.completed} files completed, ${data.skipped || 0} skipped, ${data.failed} failed.`);
              eventSource.close();
              eventSourceRef.current = null;
              break;

            case 'error':
              setError(`Download error: ${data.message}`);
              setIsDownloading(false);
              eventSource.close();
              eventSourceRef.current = null;
              break;

            default:
              console.log('Unknown event type:', data.type);
          }
        } catch (e) {
          console.error('Error parsing SSE data:', e);
        }
      };

      eventSource.onerror = (err) => {
        console.error('SSE error:', err);
        if (eventSource.readyState === EventSource.CLOSED) {
          setIsDownloading(false);
          if (!downloadComplete) {
            setError('Connection to server lost. Check your download progress.');
          }
        }
      };

    } catch (err) {
      console.error('Download error:', err);
      const errorMsg = err.response?.data?.detail || err.message || 'Unknown error';
      if (errorMsg.includes('401') || errorMsg.includes('expired') || errorMsg.includes('ssoid')) {
        setError('Session expired. Please logout and login with a fresh ssoid.');
      } else {
        setError(`Download failed: ${errorMsg}`);
      }
      setIsDownloading(false);
    }
  };

  const handleAbortDownload = async () => {
    if (!currentJobId) return;

    try {
      await axios.post(`${API_BASE}/api/abortDownloadJob/${currentJobId}`);
      setSuccess('Abort signal sent. Waiting for current operations to complete...');
    } catch (err) {
      console.error('Abort error:', err);
      setError('Failed to abort download');
    }
  };

  const handleRetryFailed = async () => {
    if (!currentJobId || failedFilesCount === 0) return;

    setError('');
    setIsDownloading(true);

    try {
      const retryResponse = await axios.post(`${API_BASE}/api/retryFailedFiles/${currentJobId}`, {
        ssoid
      });

      if (!retryResponse.data.newJobId) {
        setSuccess('No failed files to retry');
        setIsDownloading(false);
        return;
      }

      const newJobId = retryResponse.data.newJobId;
      setCurrentJobId(newJobId);
      setDownloadComplete(false);

      // Connect to SSE stream for retry job
      const eventSource = new EventSource(`${API_BASE}/api/streamDownload/${newJobId}`);
      eventSourceRef.current = eventSource;

      eventSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          switch (data.type) {
            case 'start':
              setSuccess(`Retrying ${data.totalFiles} failed files...`);
              setDownloadProgress({
                completed: 0,
                failed: 0,
                total: data.totalFiles,
                percent: 0,
                currentFile: null,
                isRetry: true
              });
              break;

            case 'progress':
              setDownloadProgress({
                completed: data.completed,
                failed: data.failed,
                total: data.total,
                percent: data.percent,
                currentFile: data.currentFile,
                isRetry: true
              });
              setFailedFilesCount(data.failed);
              break;

            case 'complete':
              setDownloadProgress({
                completed: data.completed,
                failed: data.failed,
                total: data.total,
                percent: 100,
                currentFile: null,
                bucket: data.bucket,
                isRetry: true
              });
              setDownloadComplete(true);
              setIsDownloading(false);
              setSuccess(`Retry complete! ${data.completed} files recovered, ${data.failed} still failed.`);
              eventSource.close();
              eventSourceRef.current = null;
              break;

            case 'error':
              setError(`Retry error: ${data.message}`);
              setIsDownloading(false);
              eventSource.close();
              eventSourceRef.current = null;
              break;

            default:
              console.log('Unknown event type:', data.type);
          }
        } catch (e) {
          console.error('Error parsing SSE data:', e);
        }
      };

      eventSource.onerror = () => {
        setIsDownloading(false);
      };

    } catch (err) {
      console.error('Retry error:', err);
      setError(`Retry failed: ${err.message}`);
      setIsDownloading(false);
    }
  };

  // Show intro video first
  if (!introComplete) {
    return (
      <div className="intro-screen">
        <video
          autoPlay
          muted
          playsInline
          className="intro-video"
          src="/assets/videos/chimera1-bg.mp4"
          onEnded={() => setIntroComplete(true)}
        />
      </div>
    );
  }

  if (!isLoggedIn) {
    return (
      <div className="app">
        <div className="image-bg"></div>
        <div className="login-overlay"></div>

        <div className="login-screen">
          <div className="glass-panel login-panel">
            <div className="logo-section">
              <h1 className="app-title">Ascot Wealth Management</h1>
              <p className="app-subtitle">Betfair API Explorer 2.0</p>
            </div>

            <div className="separator"></div>

            {error && <div className="error-message">{error}</div>}
            {success && <div className="success-message">{success}</div>}

            <div className="form-group">
              <label className="form-label">Session Token (ssoid)</label>
              <input
                type="password"
                className="form-input"
                placeholder="Enter your ssoid token"
                value={ssoid}
                onChange={(e) => setSsoid(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && handleConnect()}
              />
            </div>

            <button
              className="button-primary"
              onClick={handleConnect}
              disabled={loading}
            >
              {loading ? 'Connecting...' : 'Connect'}
            </button>

            <p className="copyright">© 2026 Ascot Wealth Management</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="image-bg"></div>
      <div className="login-overlay"></div>

      <div className="dashboard">
        <div className="header">
          <div className="header-left">
            <h1 className="header-title">Ascot Wealth Management</h1>
            <p className="header-subtitle">Betfair Historic Data Explorer</p>
          </div>
          <button
            className="button-logout"
            onClick={() => {
              if (eventSourceRef.current) {
                eventSourceRef.current.close();
              }
              setIsLoggedIn(false);
              setSsoid('');
              setHasChecked(false);
              setFileCount(0);
              setTotalSizeMB(0);
              setDownloadProgress(null);
              setIsDownloading(false);
            }}
            disabled={isDownloading}
          >
            Logout
          </button>
        </div>

        <div className="content">
          {error && <div className="error-message">{error}</div>}
          {success && <div className="success-message">{success}</div>}

          <div className="main-container">
            <div className="glass-panel control-panel">
              <h2 className="panel-title">Select Date Range</h2>

              <div className="date-section">
                <div className="date-group">
                  <label className="date-label">From</label>
                  <div className="date-inputs">
                    <select
                      className="date-select"
                      value={fromMonth}
                      onChange={handleDateChange(setFromMonth)}
                      disabled={isDownloading}
                    >
                      {MONTHS.map((m, i) => (
                        <option key={i} value={i + 1}>
                          {m}
                        </option>
                      ))}
                    </select>
                    <select
                      className="date-select"
                      value={fromYear}
                      onChange={handleDateChange(setFromYear)}
                      disabled={isDownloading}
                    >
                      {YEARS.map((y) => (
                        <option key={y} value={y}>
                          {y}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="date-group">
                  <label className="date-label">To</label>
                  <div className="date-inputs">
                    <select
                      className="date-select"
                      value={toMonth}
                      onChange={handleDateChange(setToMonth)}
                      disabled={isDownloading}
                    >
                      {MONTHS.map((m, i) => (
                        <option key={i} value={i + 1}>
                          {m}
                        </option>
                      ))}
                    </select>
                    <select
                      className="date-select"
                      value={toYear}
                      onChange={handleDateChange(setToYear)}
                      disabled={isDownloading}
                    >
                      {YEARS.map((y) => (
                        <option key={y} value={y}>
                          {y}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>

              <div className="tier-section">
                <label className="date-label">Data Tier</label>
                <select
                  className="tier-select"
                  value={selectedPlan}
                  onChange={(e) => {
                    setSelectedPlan(e.target.value);
                    setHasChecked(false);
                    setFileCount(0);
                    setTotalSizeMB(0);
                    setDownloadProgress(null);
                    setDownloadComplete(false);
                  }}
                  disabled={isDownloading}
                >
                  {DATA_TIERS.map((tier) => (
                    <option key={tier.value} value={tier.value}>
                      {tier.label}
                    </option>
                  ))}
                </select>
                <p className="tier-description">
                  {DATA_TIERS.find(t => t.value === selectedPlan)?.description}
                </p>
              </div>

              <div className="preset-values">
                <div className="preset-item">
                  <span className="preset-label">Sport:</span>
                  <span className="preset-value">Horse Racing</span>
                </div>
                <div className="preset-item">
                  <span className="preset-label">Country:</span>
                  <span className="preset-value">GB</span>
                </div>
                <div className="preset-item">
                  <span className="preset-label">File Types:</span>
                  <span className="preset-value">M & E</span>
                </div>
                <div className="preset-item">
                  <span className="preset-label">Market Types:</span>
                  <span className="preset-value">All</span>
                </div>
              </div>

              <button
                className="button-check"
                onClick={handleCheckAvailability}
                disabled={loading || isDownloading}
              >
                {loading ? 'Checking...' : 'Check File Availability'}
              </button>
            </div>

            {hasChecked && (
              <div className="glass-panel results-panel">
                <h2 className="panel-title">Results</h2>

                <div className="results-grid">
                  <div className="result-card">
                    <div className="result-icon">📁</div>
                    <div className="result-value">{fileCount.toLocaleString()}</div>
                    <div className="result-label">Files Available</div>
                  </div>
                  <div className="result-card">
                    <div className="result-icon">💾</div>
                    <div className="result-value">{totalSizeMB.toLocaleString()}</div>
                    <div className="result-label">MB Total</div>
                  </div>
                </div>

                {/* Progress Section */}
                {downloadProgress && (
                  <div className="progress-section">
                    <div className="progress-header">
                      <span className="progress-title">
                        {downloadProgress.isRetry ? 'Retry Progress' : 'Download Progress'}
                      </span>
                      <span className="progress-percent">{downloadProgress.percent}%</span>
                    </div>
                    <div className="progress-bar-container">
                      <div
                        className="progress-bar"
                        style={{ width: `${downloadProgress.percent}%` }}
                      />
                    </div>
                    <div className="progress-details">
                      <div className="progress-stat">
                        <span className="stat-label">Completed:</span>
                        <span className="stat-value success">{downloadProgress.completed.toLocaleString()}</span>
                      </div>
                      {downloadProgress.skipped > 0 && (
                        <div className="progress-stat">
                          <span className="stat-label">Skipped:</span>
                          <span className="stat-value skipped">{downloadProgress.skipped.toLocaleString()}</span>
                        </div>
                      )}
                      <div className="progress-stat">
                        <span className="stat-label">Failed:</span>
                        <span className="stat-value error">{downloadProgress.failed.toLocaleString()}</span>
                      </div>
                      <div className="progress-stat">
                        <span className="stat-label">Total:</span>
                        <span className="stat-value">{downloadProgress.total.toLocaleString()}</span>
                      </div>
                    </div>
                    {downloadProgress.currentFile && isDownloading && (
                      <div className="current-file">
                        Processing: {downloadProgress.currentFile}
                      </div>
                    )}
                  </div>
                )}

                {/* Download Controls */}
                <div className="download-section">
                  {!isDownloading && !downloadComplete && (
                    <button
                      className="button-download"
                      onClick={handleStartDownload}
                      disabled={loading}
                    >
                      Start Download to Cloud Storage
                    </button>
                  )}

                  {isDownloading && (
                    <button
                      className="button-abort"
                      onClick={handleAbortDownload}
                    >
                      Abort Download
                    </button>
                  )}

                  {downloadComplete && failedFilesCount > 0 && (
                    <button
                      className="button-retry"
                      onClick={handleRetryFailed}
                      disabled={isDownloading}
                    >
                      Retry {failedFilesCount} Failed Files
                    </button>
                  )}

                  {downloadComplete && failedFilesCount === 0 && (
                    <div className="download-complete">
                      All files uploaded successfully!
                    </div>
                  )}

                  {/* Reset Button - shown when there's progress data and not currently downloading */}
                  {downloadProgress && !isDownloading && (
                    <button
                      className="button-reset"
                      onClick={() => {
                        setDownloadProgress(null);
                        setCurrentJobId(null);
                        setFailedFilesCount(0);
                        setDownloadComplete(false);
                        setSuccess('');
                        setError('');
                      }}
                    >
                      Reset / New Download
                    </button>
                  )}
                </div>

                {/* Stats Summary */}
                {downloadProgress && downloadProgress.bucket && (
                  <div className="download-stats">
                    <h3>Upload Summary</h3>
                    <div className="stats-row">
                      <span>Destination:</span>
                      <span>gs://{downloadProgress.bucket}/</span>
                    </div>
                    <div className="stats-row">
                      <span>Date Range:</span>
                      <span>{MONTHS[parseInt(fromMonth)-1]} {fromYear} - {MONTHS[parseInt(toMonth)-1]} {toYear}</span>
                    </div>
                    <div className="stats-row">
                      <span>Files Uploaded:</span>
                      <span>{downloadProgress.completed.toLocaleString()}</span>
                    </div>
                    {downloadProgress.skipped > 0 && (
                      <div className="stats-row">
                        <span>Files Skipped (already exist):</span>
                        <span>{downloadProgress.skipped.toLocaleString()}</span>
                      </div>
                    )}
                    <div className="stats-row">
                      <span>Files Failed:</span>
                      <span>{downloadProgress.failed.toLocaleString()}</span>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
