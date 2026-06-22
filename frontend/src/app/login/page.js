'use client';
import React, { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { apiRequest } from '@/services/api';

export default function LoginPage() {
  const router = useRouter();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState('');

  // Clear any old session details when visiting login page
  useEffect(() => {
    localStorage.removeItem('accessToken');
    localStorage.removeItem('refreshToken');
    localStorage.removeItem('username');
  }, []);

  const handleLogin = async (e) => {
    e.preventDefault();
    if (!username || !password) {
      setError('Por favor complete todos los campos.');
      return;
    }

    setError('');
    setLoading(true);

    try {
      const response = await apiRequest('/auth/login', {
        method: 'POST',
        body: { username, password }
      });

      if (response && response.access_token) {
        localStorage.setItem('accessToken', response.access_token);
        localStorage.setItem('refreshToken', response.refresh_token);
        localStorage.setItem('username', username);
        router.push('/');
      } else {
        setError('Error al recibir tokens de sesión.');
      }
    } catch (err) {
      console.error('Login error:', err);
      setError(err.message || 'Error al iniciar sesión. Verifique sus credenciales.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      minHeight: '100vh',
      width: '100%',
      background: 'var(--background-gradient)',
      padding: '1.5rem'
    }}>
      <div style={{
        background: 'rgba(255, 255, 255, 0.05)',
        backdropFilter: 'blur(16px) saturate(180%)',
        WebkitBackdropFilter: 'blur(16px) saturate(180%)',
        border: '1px solid rgba(255, 255, 255, 0.1)',
        borderRadius: 'var(--border-radius)',
        padding: '2.5rem',
        width: '100%',
        maxWidth: '420px',
        boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)',
        color: '#fff',
        fontFamily: 'var(--font-family)',
        display: 'flex',
        flexDirection: 'column',
        gap: '1.5rem'
      }}>
        {/* Header Branding */}
        <div style={{ textAlign: 'center' }}>
          <h1 style={{
            fontSize: '2rem',
            fontWeight: '700',
            margin: 0,
            letterSpacing: '-0.02em',
            background: 'linear-gradient(to right, #ffffff, rgba(255,255,255,0.7))',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent'
          }}>
            VibeCloud <span style={{ color: 'var(--primary-color)', WebkitTextFillColor: 'initial' }}>SaaS</span>
          </h1>
          <p style={{
            fontSize: '0.85rem',
            opacity: 0.7,
            marginTop: '0.5rem'
          }}>
            Portal de Acceso Mayorista adaptativo
          </p>
        </div>

        {/* Login Form */}
        <form onSubmit={handleLogin} style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
            <label style={{ fontSize: '0.8rem', fontWeight: '600', opacity: 0.85 }}>Usuario</label>
            <input
              type="text"
              placeholder="Ingrese su usuario"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              style={{
                background: 'rgba(0, 0, 0, 0.25)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                padding: '0.8rem 1rem',
                borderRadius: 'calc(var(--border-radius) / 2)',
                color: '#fff',
                fontSize: '0.95rem',
                outline: 'none',
                fontFamily: 'var(--font-family)',
                transition: 'border-color 0.2s'
              }}
              onFocus={(e) => e.target.style.borderColor = 'var(--primary-color)'}
              onBlur={(e) => e.target.style.borderColor = 'rgba(255, 255, 255, 0.1)'}
            />
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
            <label style={{ fontSize: '0.8rem', fontWeight: '600', opacity: 0.85 }}>Contraseña</label>
            <input
              type="password"
              placeholder="••••••••"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              style={{
                background: 'rgba(0, 0, 0, 0.25)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                padding: '0.8rem 1rem',
                borderRadius: 'calc(var(--border-radius) / 2)',
                color: '#fff',
                fontSize: '0.95rem',
                outline: 'none',
                fontFamily: 'var(--font-family)',
                transition: 'border-color 0.2s'
              }}
              onFocus={(e) => e.target.style.borderColor = 'var(--primary-color)'}
              onBlur={(e) => e.target.style.borderColor = 'rgba(255, 255, 255, 0.1)'}
            />
          </div>

          {error && (
            <div style={{
              color: '#ef4444',
              background: 'rgba(239, 68, 68, 0.1)',
              border: '1px solid rgba(239, 68, 68, 0.2)',
              borderRadius: '6px',
              padding: '0.6rem 0.8rem',
              fontSize: '0.8rem',
              fontWeight: '500'
            }}>
              ⚠️ {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            style={{
              background: 'var(--primary-color)',
              color: '#fff',
              border: 'none',
              padding: '0.9rem',
              borderRadius: 'calc(var(--border-radius) / 2)',
              cursor: loading ? 'not-allowed' : 'pointer',
              fontWeight: '700',
              fontSize: '1rem',
              fontFamily: 'var(--font-family)',
              marginTop: '0.5rem',
              transition: 'opacity 0.2s',
              opacity: loading ? 0.7 : 1
            }}
            onMouseEnter={(e) => { if (!loading) e.target.style.opacity = 0.9; }}
            onMouseLeave={(e) => { if (!loading) e.target.style.opacity = 1; }}
          >
            {loading ? 'Iniciando Sesión...' : 'Ingresar'}
          </button>
        </form>

        <div style={{
          textAlign: 'center',
          fontSize: '0.75rem',
          opacity: 0.5,
          marginTop: '0.5rem'
        }}>
          VibeCloud SaaS &copy; 2026. Todos los derechos reservados.
        </div>
      </div>
    </div>
  );
}
