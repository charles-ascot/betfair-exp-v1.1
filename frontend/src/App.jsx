import React, { useState } from 'react';
import axios from 'axios';
import './App.css';

const API_BASE = 'https://betfair-backend-1026419041222.us-central1.run.app';

export default function App() {
  const [ssoid, setSsoid] = useState('');
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // Form state
  const [fromDay, setFromDay] = useState('1');
  const [fromMonth, setFromMonth] = useState('1');
  const [fromYear, setFromYear] = useState('2024');
  const [toDay, setToDay] = useState('31');
  const [toMonth, setToMonth] = useState('12');
  const [toYear, setToYear] = useState('2024');

  const [selectedMarkets, setSelectedMarkets] = useState([]);
  const [selectedCountries, setSelectedCountries] = useState([]);
  const [selectedFileTypes, setSelectedFileTypes] = useState([]);

  const [markets, setMarkets] = useState([]);
  const [countries, setCountries] = useState([]);
  const [fileTypes, setFileTypes] = useState([]);

  const [fileCount, setFileCount] = useState(0);
  const [totalSizeMB, setTotalSizeMB] = useState(0);

  const [fileList, setFileList] = useState([]);
  const [showFileList, setShowFileList] = useState(false);

  const handleConnect = async () => {
    if (!ssoid.trim()) {
      setError('Please enter your ssoid token');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const response = await axios.post(`${API_BASE}/api/GetMyData`, { ssoid });
      if (response.status === 200) {
        setIsLoggedIn(true);
      }
    } catch (err) {
      setError('Failed to authenticate. Check your ssoid token.');
    } finally {
      setLoading(false);
    }
  };

  const handleGetFilterOptions = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await axios.post(`${API_BASE}/api/getCollectionOptions`, {
        ssoid,
        sport: 'Horse Racing',
        plan: 'Basic Plan',
        fromDay: parseInt(fromDay),
        fromMonth: parseInt(fromMonth),
        fromYear: parseInt(fromYear),
        toDay: parseInt(toDay),
        toMonth: parseInt(toMonth),
        toYear: parseInt(toYear),
        marketTypesCollection: [],
        countriesCollection: [],
        fileTypeCollection: []
      });

      setMarkets(response.data.marketTypesCollection || []);
      setCountries(response.data.countriesCollection || []);
      setFileTypes(response.data.fileTypeCollection || []);
    } catch (err) {
      setError('Failed to fetch filter options');
    } finally {
      setLoading(false);
    }
  };

  const handleGetBasketSize = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await axios.post(`${API_BASE}/api/getAdvBasketDataSize`, {
        ssoid,
        sport: 'Horse Racing',
        plan: 'Basic Plan',
        fromDay: parseInt(fromDay),
        fromMonth: parseInt(fromMonth),
        fromYear: parseInt(fromYear),
        toDay: parseInt(toDay),
        toMonth: parseInt(toMonth),
        toYear: parseInt(toYear),
        marketTypesCollection: selectedMarkets,
        countriesCollection: selectedCountries,
        fileTypeCollection: selectedFileTypes
      });

      setFileCount(response.data.fileCount);
      setTotalSizeMB(response.data.totalSizeMB);
    } catch (err) {
      setError('Failed to calculate size');
    } finally {
      setLoading(false);
    }
  };

  const handleGetFileList = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await axios.post(`${API_BASE}/api/downloadListOfFiles`, {
        ssoid,
        sport: 'Horse Racing',
        plan: 'Basic Plan',
        fromDay: parseInt(fromDay),
        fromMonth: parseInt(fromMonth),
        fromYear: parseInt(fromYear),
        toDay: parseInt(toDay),
        toMonth: parseInt(toMonth),
        toYear: parseInt(toYear),
        marketTypesCollection: selectedMarkets,
        countriesCollection: selectedCountries,
        fileTypeCollection: selectedFileTypes
      });

      setFileList(response.data);
      setShowFileList(true);
    } catch (err) {
      setError('Failed to fetch file list');
    } finally {
      setLoading(false);
    }
  };

  if (!isLoggedIn) {
    return (
      <div className="app">
        <div className="login-screen">
          <div className="login-container">
            <div className="login-header">Betfair Historic Data</div>
            <div className="login-subtitle">Session Token Login</div>

            {error && <div className="error-message">{error}</div>}

            <div className="form-group">
              <label>Session Token (ssoid)</label>
              <input
                type="password"
                placeholder="Enter your ssoid token"
                value={ssoid}
                onChange={(e) => setSsoid(e.target.value)}
              />
            </div>

            <button
              className="connect-button"
              onClick={handleConnect}
              disabled={loading}
            >
              {loading ? 'Connecting...' : 'Connect'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <div className="dashboard">
        <div className="header">
          <div className="header-title">Betfair Historic Data Explorer</div>
          <button
            className="logout-button"
            onClick={() => {
              setIsLoggedIn(false);
              setSsoid('');
            }}
          >
            Logout
          </button>
        </div>

        <div className="content">
          {error && <div className="error-message">{error}</div>}

          <div className="grid-2">
            <div className="card">
              <div className="card-title">Date Range</div>
              <div>
                <div style={{ marginBottom: '12px' }}>
                  <label style={{ color: '#888', fontSize: '12px' }}>From</label>
                  <div className="date-inputs">
                    <input
                      type="number"
                      min="1"
                      max="31"
                      value={fromDay}
                      onChange={(e) => setFromDay(e.target.value)}
                      placeholder="Day"
                    />
                    <input
                      type="number"
                      min="1"
                      max="12"
                      value={fromMonth}
                      onChange={(e) => setFromMonth(e.target.value)}
                      placeholder="Month"
                    />
                    <input
                      type="number"
                      min="2000"
                      value={fromYear}
                      onChange={(e) => setFromYear(e.target.value)}
                      placeholder="Year"
                    />
                  </div>
                </div>
                <div>
                  <label style={{ color: '#888', fontSize: '12px' }}>To</label>
                  <div className="date-inputs">
                    <input
                      type="number"
                      min="1"
                      max="31"
                      value={toDay}
                      onChange={(e) => setToDay(e.target.value)}
                      placeholder="Day"
                    />
                    <input
                      type="number"
                      min="1"
                      max="12"
                      value={toMonth}
                      onChange={(e) => setToMonth(e.target.value)}
                      placeholder="Month"
                    />
                    <input
                      type="number"
                      min="2000"
                      value={toYear}
                      onChange={(e) => setToYear(e.target.value)}
                      placeholder="Year"
                    />
                  </div>
                </div>
              </div>
            </div>

            <div className="card">
              <div className="card-title">Market Selection</div>
              <div style={{ marginBottom: '12px' }}>
                <label style={{ color: '#888', fontSize: '12px' }}>Sport</label>
                <select className="dropdown" disabled>
                  <option>Horse Racing</option>
                </select>
              </div>
              <div>
                <label style={{ color: '#888', fontSize: '12px' }}>Plan</label>
                <select className="dropdown" disabled>
                  <option>Basic Plan</option>
                </select>
              </div>
            </div>
          </div>

          <div className="grid-2">
            <div className="card">
              <div className="card-title">Market Types</div>
              <div className="checkbox-group">
                {markets.map((market) => (
                  <div key={market.name} className="checkbox-item">
                    <input
                      type="checkbox"
                      id={`market-${market.name}`}
                      checked={selectedMarkets.includes(market.name)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedMarkets([...selectedMarkets, market.name]);
                        } else {
                          setSelectedMarkets(selectedMarkets.filter((m) => m !== market.name));
                        }
                      }}
                    />
                    <label htmlFor={`market-${market.name}`}>
                      {market.name} ({market.count.toLocaleString()})
                    </label>
                  </div>
                ))}
              </div>
            </div>

            <div className="card">
              <div className="card-title">Countries</div>
              <div className="checkbox-group">
                {countries.map((country) => (
                  <div key={country.name} className="checkbox-item">
                    <input
                      type="checkbox"
                      id={`country-${country.name}`}
                      checked={selectedCountries.includes(country.name)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedCountries([...selectedCountries, country.name]);
                        } else {
                          setSelectedCountries(selectedCountries.filter((c) => c !== country.name));
                        }
                      }}
                    />
                    <label htmlFor={`country-${country.name}`}>
                      {country.name} ({country.count.toLocaleString()})
                    </label>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="grid-2">
            <div className="card">
              <div className="card-title">File Types</div>
              <div className="checkbox-group">
                {fileTypes.map((fileType) => (
                  <div key={fileType.name} className="checkbox-item">
                    <input
                      type="checkbox"
                      id={`filetype-${fileType.name}`}
                      checked={selectedFileTypes.includes(fileType.name)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedFileTypes([...selectedFileTypes, fileType.name]);
                        } else {
                          setSelectedFileTypes(selectedFileTypes.filter((f) => f !== fileType.name));
                        }
                      }}
                    />
                    <label htmlFor={`filetype-${fileType.name}`}>
                      Type {fileType.name} ({fileType.count.toLocaleString()})
                    </label>
                  </div>
                ))}
              </div>
            </div>

            <div className="card">
              <div className="card-title">Summary</div>
              <div className="stat-grid">
                <div className="stat">
                  <div className="stat-value">{fileCount.toLocaleString()}</div>
                  <div className="stat-label">Files</div>
                </div>
                <div className="stat">
                  <div className="stat-value">{totalSizeMB.toLocaleString()}</div>
                  <div className="stat-label">MB</div>
                </div>
              </div>
              <div className="buttons">
                <button
                  className="button"
                  onClick={handleGetFilterOptions}
                  disabled={loading}
                >
                  {loading ? '...' : 'Load Options'}
                </button>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-title">Actions</div>
            <div className="buttons">
              <button
                className="button"
                onClick={handleGetBasketSize}
                disabled={loading}
              >
                {loading ? 'Computing...' : 'Calculate Size'}
              </button>
              <button
                className="button button-primary"
                onClick={handleGetFileList}
                disabled={loading}
              >
                {loading ? 'Loading...' : 'Get File List'}
              </button>
            </div>
          </div>

          {showFileList && fileList.length > 0 && (
            <div className="card">
              <div className="card-title">File List ({fileList.length} files)</div>
              <div className="file-list">
                {fileList.map((file, idx) => (
                  <div key={idx} className="file-item">{file}</div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}