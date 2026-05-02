import { useState, useRef } from 'react';
import { DecisionGraph, JdmConfigProvider, DecisionGraphType } from '@gorules/jdm-editor';
import { api } from './api/client';
import './App.css';

// Initial empty graph
const initialGraph: DecisionGraphType = { nodes: [], edges: [] };

function App() {
  const [graph, setGraph] = useState<DecisionGraphType>(initialGraph);
  const [currentFile, setCurrentFile] = useState<string | null>(null);
  const [fileList, setFileList] = useState<string[]>([]);
  const [isVolumeModalOpen, setIsVolumeModalOpen] = useState(false);
  const [status, setStatus] = useState<string>('');

  const fileInputRef = useRef<HTMLInputElement>(null);

  // --- Actions ---

  const handleNew = () => {
    if (confirm('Create new rule? Unsaved changes will be lost.')) {
      setGraph(initialGraph);
      setCurrentFile(null);
      setStatus('New rule created');
    }
  };

  const handleOpenVolumeClick = async () => {
    try {
      const files = await api.listRules();
      setFileList(files);
      setIsVolumeModalOpen(true);
    } catch (e) {
      alert('Failed to list files from volume');
      console.error(e);
    }
  };

  const handleVolumeFileSelect = async (filename: string) => {
    try {
      const content = await api.getRule(filename);
      setGraph(content);
      setCurrentFile(filename);
      setIsVolumeModalOpen(false);
      setStatus(`Opened ${filename} from Volume`);
    } catch (e) {
      alert(`Failed to open ${filename}`);
      console.error(e);
    }
  };

  const handleFileSystemClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (event) => {
      try {
        const content = JSON.parse(event.target?.result as string);
        setGraph(content);
        setCurrentFile(file.name); // Keep name but it's local
        setStatus(`Opened ${file.name} from File System`);
      } catch (err) {
        alert('Invalid JSON file');
      }
    };
    reader.readAsText(file);
    // Reset input
    e.target.value = '';
  };

  const handleSave = async () => {
    let filename = currentFile;
    if (!filename) {
      const input = prompt('Enter filename to save to Volume (e.g. my-rule.json):');
      if (!input) return;
      filename = input;
    }

    try {
      setStatus('Saving...');
      await api.saveRule(filename, graph);
      setCurrentFile(filename);
      setStatus(`Saved ${filename} to Volume`);
    } catch (e) {
      alert('Failed to save');
      setStatus('Save failed');
      console.error(e);
    }
  };

  return (
    <div className="app-container">
      {/* Header / Menu Bar */}
      <div className="menubar">
        <div className="brand">CEP Rules Editor</div>
        <div className="menu-group">
          <button onClick={handleNew}>New</button>

          <div className="dropdown">
            <button className="dropbtn">Open ▼</button>
            <div className="dropdown-content">
              <a onClick={handleOpenVolumeClick}>Volume (Rules Apps)</a>
              <a onClick={handleFileSystemClick}>File System</a>
            </div>
          </div>

          <button onClick={handleSave}>Save</button>
        </div>
        <div className="status-bar">{currentFile ? `Editing: ${currentFile}` : 'New Rule'} | {status}</div>
      </div>

      {/* Editor */}
      <div className="editor-wrapper">
        <JdmConfigProvider>
          <DecisionGraph value={graph} onChange={setGraph} />
        </JdmConfigProvider>
      </div>

      {/* Hidden File Input */}
      <input
        type="file"
        ref={fileInputRef}
        style={{ display: 'none' }}
        accept=".json"
        onChange={handleFileChange}
      />

      {/* Volume File List Modal */}
      {isVolumeModalOpen && (
        <div className="modal-overlay">
          <div className="modal">
            <h3>Open from Volume</h3>
            <div className="file-list">
              {fileList.length === 0 ? <p>No files found.</p> : (
                <ul>
                  {fileList.map(f => (
                    <li key={f} onClick={() => handleVolumeFileSelect(f)}>
                      {f}
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <button className="close-btn" onClick={() => setIsVolumeModalOpen(false)}>Cancel</button>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
