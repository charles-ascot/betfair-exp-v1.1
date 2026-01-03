import React, { useState } from 'react';
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

  // Pre-selected defaults (hardcoded)
  const selectedMarkets = []; // All market types
  const selectedCountries = ['GB']; // Only GB
  const selectedFileTypes = ['M', 'E']; // Both M and E

  const [fileCount, setFileCount] = useState(0);
  const [totalSizeMB, setTotalSizeMB] = useState(0);
  const [hasChecked, setHasChecked] = useState(false);

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
        plan: 'Basic Plan',
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
      setSuccess(`Found ${response.data.fileCount} files, ${response.data.totalSizeMB} MB total`);
      setTimeout(() => setSuccess(''), 4000);
    } catch (err) {
      setError('Failed to check availability. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = async () => {
    setLoading(true);
    setError('');
    setSuccess('');

    try {
      // First get the list of files
      const listResponse = await axios.post(`${API_BASE}/api/downloadListOfFiles`, {
        ssoid,
        sport: 'Horse Racing',
        plan: 'Basic Plan',
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
        return;
      }

      // Warn if there are many files
      const totalFiles = filePaths.length;
      const downloadingFiles = Math.min(totalFiles, 500);
      if (totalFiles > 500) {
        setSuccess(`Downloading first ${downloadingFiles} of ${totalFiles} files... (limit 500 per batch)`);
      } else {
        setSuccess(`Downloading ${downloadingFiles} files... This may take a moment.`);
      }

      // Now download the files as a ZIP
      const downloadResponse = await axios.post(
        `${API_BASE}/api/downloadFiles`,
        { ssoid, filePaths },
        { responseType: 'blob', timeout: 600000 }  // 10 minute timeout
      );

      // Check if response is actually a ZIP or an error
      if (downloadResponse.data.size < 1000) {
        // Tiny response - likely an error
        const text = await downloadResponse.data.text();
        if (text.includes('error') || text.includes('expired')) {
          setError('Session expired. Please logout and login with a new ssoid.');
          return;
        }
      }

      // Create a download link and trigger it
      const blob = new Blob([downloadResponse.data], { type: 'application/zip' });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'betfair_historic_data.zip';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

      setSuccess(`Download complete! ${downloadingFiles} files (${(downloadResponse.data.size / 1024 / 1024).toFixed(1)} MB)`);
      setTimeout(() => setSuccess(''), 6000);
    } catch (err) {
      console.error('Download error:', err);
      const errorMsg = err.response?.data?.detail || err.message || 'Unknown error';
      setError(`Failed to download: ${errorMsg}. Try a smaller date range or refresh your session.`);
    } finally {
      setLoading(false);
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
              setIsLoggedIn(false);
              setSsoid('');
              setHasChecked(false);
              setFileCount(0);
              setTotalSizeMB(0);
            }}
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
                      onChange={(e) => setFromMonth(e.target.value)}
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
                      onChange={(e) => setFromYear(e.target.value)}
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
                      onChange={(e) => setToMonth(e.target.value)}
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
                      onChange={(e) => setToYear(e.target.value)}
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

              <div className="preset-values">
                <div className="preset-item">
                  <span className="preset-label">Sport:</span>
                  <span className="preset-value">Horse Racing ✓</span>
                </div>
                <div className="preset-item">
                  <span className="preset-label">Country:</span>
                  <span className="preset-value">GB ✓</span>
                </div>
                <div className="preset-item">
                  <span className="preset-label">File Types:</span>
                  <span className="preset-value">M & E ✓</span>
                </div>
                <div className="preset-item">
                  <span className="preset-label">Market Types:</span>
                  <span className="preset-value">All ✓</span>
                </div>
              </div>

              <button
                className="button-check"
                onClick={handleCheckAvailability}
                disabled={loading}
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
                    <div className="result-label">Files</div>
                  </div>
                  <div className="result-card">
                    <div className="result-icon">💾</div>
                    <div className="result-value">{totalSizeMB.toLocaleString()}</div>
                    <div className="result-label">MB</div>
                  </div>
                </div>

                <div className="download-section">
                  <button
                    className="button-download"
                    onClick={handleDownload}
                    disabled={loading}
                  >
                    {loading ? 'Downloading...' : '⬇️ Download Files'}
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
