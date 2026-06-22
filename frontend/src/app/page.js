'use client';
import React, { useState, useEffect, useCallback } from 'react';
import { useRouter } from 'next/navigation';
import { apiRequest } from '@/services/api';
import ThemeInjector from '@/components/ThemeInjector';
import DynamicViewRenderer from '@/components/DynamicViewRenderer';

// Simple JWT decoder to extract claims on client-side without third party libs
function decodeJwt(token) {
  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const jsonPayload = decodeURIComponent(atob(base64).split('').map(function(c) {
      return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
    }).join(''));
    return JSON.parse(jsonPayload);
  } catch (e) {
    return null;
  }
}

export default function Home() {
  const router = useRouter();
  const [user, setUser] = useState(null);
  const [uiConfig, setUiConfig] = useState(null);
  const [products, setProducts] = useState([]);
  const [page, setPage] = useState(1);
  const [pages, setPages] = useState(1);
  const [totalProducts, setTotalProducts] = useState(0);
  const [searchValue, setSearchValue] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [cartItems, setCartItems] = useState([]);
  const [sales, setSales] = useState([]);
  const [processingSale, setProcessingSale] = useState(false);
  const [loadingUI, setLoadingUI] = useState(true);

  // Authenticate user & extract details
  useEffect(() => {
    const token = localStorage.getItem('accessToken');
    if (!token) {
      router.push('/login');
      return;
    }

    const payload = decodeJwt(token);
    const storedUsername = localStorage.getItem('username') || 'Usuario';
    if (!payload) {
      localStorage.removeItem('accessToken');
      localStorage.removeItem('refreshToken');
      router.push('/login');
      return;
    }

    setUser({
      id: payload.sub,
      tenant_id: payload.tenant_id,
      role: payload.role,
      username: storedUsername,
      full_name: storedUsername.charAt(0).toUpperCase() + storedUsername.slice(1)
    });
  }, [router]);

  // Fetch sales history
  const fetchSales = useCallback(async () => {
    try {
      const res = await apiRequest('/sales?page=1&limit=10');
      if (res && res.items) {
        setSales(res.items);
      }
    } catch (err) {
      console.error('Error fetching sales:', err);
    }
  }, []);

  // Fetch product catalog
  const fetchProducts = useCallback(async () => {
    try {
      const url = `/products?page=${page}&limit=12${searchQuery ? `&search=${encodeURIComponent(searchQuery)}` : ''}`;
      const res = await apiRequest(url);
      if (res) {
        setProducts(res.items || []);
        setPages(res.pages || 1);
        setTotalProducts(res.total || 0);
      }
    } catch (err) {
      console.error('Error fetching products:', err);
    }
  }, [page, searchQuery]);

  // Load configuration & initial session sales
  useEffect(() => {
    if (!user) return;

    let active = true;
    const loadInitialData = async () => {
      try {
        setLoadingUI(true);
        const config = await apiRequest('/ui-config/pos');
        if (config && active) {
          setUiConfig(config);
        }
        await fetchSales();
      } catch (err) {
        console.error('Error loading initial configuration:', err);
      } finally {
        if (active) setLoadingUI(false);
      }
    };

    loadInitialData();
    return () => { active = false; };
  }, [user, fetchSales]);

  // Load products based on pagination & search
  useEffect(() => {
    if (!user) return;
    fetchProducts();
  }, [user, fetchProducts]);

  // Logout handler
  const handleLogout = async () => {
    const refreshToken = localStorage.getItem('refreshToken');
    if (refreshToken) {
      try {
        await apiRequest('/auth/logout', {
          method: 'POST',
          body: { refresh_token: refreshToken }
        });
      } catch (err) {
        console.error('Logout API error:', err);
      }
    }
    localStorage.removeItem('accessToken');
    localStorage.removeItem('refreshToken');
    localStorage.removeItem('username');
    router.push('/login');
  };

  // Search trigger
  const handleSearch = () => {
    setPage(1);
    setSearchQuery(searchValue);
  };

  // Pagination trigger
  const handlePageChange = (newPage) => {
    if (newPage >= 1 && newPage <= pages) {
      setPage(newPage);
    }
  };

  // Add item to cart
  const handleAddToCart = (product) => {
    setCartItems((prev) => {
      const existing = prev.find((item) => item.product.id === product.id);
      if (existing) {
        if (existing.quantity >= product.stock_quantity) {
          return prev;
        }
        return prev.map((item) =>
          item.product.id === product.id
            ? { ...item, quantity: item.quantity + 1 }
            : item
        );
      }
      return [...prev, { product, quantity: 1, price_type: 'retail' }];
    });
  };

  // Modify quantity of item in cart
  const handleUpdateQuantity = (productId, quantity) => {
    if (quantity <= 0) {
      handleRemoveItem(productId);
      return;
    }
    setCartItems((prev) =>
      prev.map((item) => {
        if (item.product.id === productId) {
          const max = item.product.stock_quantity;
          return { ...item, quantity: Math.min(quantity, max) };
        }
        return item;
      })
    );
  };

  // Toggle price type
  const handleUpdatePriceType = (productId, priceType) => {
    setCartItems((prev) =>
      prev.map((item) =>
        item.product.id === productId
          ? { ...item, price_type: priceType }
          : item
      )
    );
  };

  // Remove single item from cart
  const handleRemoveItem = (productId) => {
    setCartItems((prev) => prev.filter((item) => item.product.id !== productId));
  };

  // Empty cart
  const handleClearCart = () => {
    setCartItems([]);
  };

  // Submit checkout
  const handleProcessSale = async (payload) => {
    if (cartItems.length === 0) return;
    setProcessingSale(true);
    try {
      const items = cartItems.map((item) => ({
        product_id: item.product.id,
        quantity: item.quantity,
        price_type: item.price_type
      }));

      const requestData = {
        items,
        client_id: payload.client_id,
        amount_paid: payload.amount_paid,
        payment_method: payload.payment_method,
        split_cash: payload.split_cash,
        split_transfer: payload.split_transfer
      };

      const res = await apiRequest('/sales', {
        method: 'POST',
        body: requestData
      });

      if (res) {
        setCartItems([]);
        alert(`¡Venta registrada con éxito! ID: ${res.id}`);
        // Refresh catalog quantities & recent sales
        await fetchProducts();
        await fetchSales();
      }
    } catch (err) {
      alert(`Error al procesar venta: ${err.message}`);
    } finally {
      setProcessingSale(false);
    }
  };

  if (loadingUI) {
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: '100vh',
        background: 'linear-gradient(135deg, #0f172a, #1e1b4b)',
        color: '#fff',
        fontFamily: 'var(--font-family, sans-serif)',
        fontSize: '1.2rem',
        fontWeight: '600'
      }}>
        Cargando interfaz adaptativa de VibeCloud...
      </div>
    );
  }

  return (
    <main style={{
      minHeight: '100vh',
      width: '100%',
      display: 'flex',
      flexDirection: 'column',
      background: 'var(--background-gradient)',
      transition: 'background 0.3s ease'
    }}>
      {uiConfig && <ThemeInjector theme={uiConfig.theme} />}
      <DynamicViewRenderer
        layout={uiConfig?.layout}
        user={user}
        onLogout={handleLogout}
        searchValue={searchValue}
        onSearchValueChange={setSearchValue}
        onSearch={handleSearch}
        products={products}
        page={page}
        pages={pages}
        totalProducts={totalProducts}
        onPageChange={handlePageChange}
        onAddToCart={handleAddToCart}
        cartItems={cartItems}
        onUpdateQuantity={handleUpdateQuantity}
        onUpdatePriceType={handleUpdatePriceType}
        onRemoveItem={handleRemoveItem}
        onClearCart={handleClearCart}
        onProcessSale={handleProcessSale}
        processing={processingSale}
        sales={sales}
      />
    </main>
  );
}
