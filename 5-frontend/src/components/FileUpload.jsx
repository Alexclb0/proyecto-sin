import React, { useState, useEffect, useCallback } from 'react';

const FileUpload = ({ setIsProcessing, onComplete }) => {
  const [file, setFile] = useState(null);
  const [fileName, setFileName] = useState('');
  const [error, setError] = useState('');
  const [uploadStatus, setUploadStatus] = useState({ status: 'idle', message: '' });
  const [taskId, setTaskId] = useState(null);

    if (taskId && (uploadStatus.status === 'procesando' || uploadStatus.status === 'subiendo')) {
      const interval = setInterval(async () => {
        try {
          const response = await fetch(`${import.meta.env.VITE_API_URL}/upload/status/${taskId}`);
          const data = await response.json();
          setUploadStatus(data);
          if (data.status === 'completado' || data.status === 'error') {
            setIsProcessing(false);
            if (data.status === 'completado') {
              setFile(null);
              setFileName('');
              if (onComplete) {
                onComplete();
              }
            }
          }
        } catch (err) {
    e.preventDefault();
    if (!file) return;

    const formData = new FormData(); // destination is now handled by the backend
    formData.append('file', file);
    setError('');
    setUploadStatus({ status: 'subiendo', message: 'Subiendo archivo al servidor...' });
    setIsProcessing(true);
    try {
      const response = await fetch(
        `${import.meta.env.VITE_API_URL}/upload`,
        { method: 'POST', body: formData }
      );

        <p className="text-sm text-white/60 mt-4">Solo se aceptan archivos .csv</p>
      </div>

      <button
        type="submit"
        className="w-full h-16 bg-primary text-white font-bold text-lg rounded-lg flex items-center justify-center gap-3 hover:bg-primary-dark transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed"

