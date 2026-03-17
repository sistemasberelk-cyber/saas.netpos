let cart = [];
let allProducts = [];
let allClients = [];

function buildLineKey(productId, priceKey) {
    return `${productId}:${priceKey}`;
}

document.addEventListener('DOMContentLoaded', async () => {
    loadCartState();

    const res = await fetch('/api/products');
    allProducts = await res.json();

    try {
        const resClients = await fetch('/api/clients');
        if (resClients.ok) {
            allClients = await resClients.json();
            const clientSelect = document.getElementById('client-select');
            if (clientSelect) {
                clientSelect.innerHTML = '<option value="">Cliente casual</option>';
                allClients.forEach(c => {
                    const opt = document.createElement('option');
                    opt.value = c.id;
                    opt.textContent = c.name;
                    clientSelect.appendChild(opt);
                });
            }
        }
    } catch (err) {
        console.error('Error loading clients:', err);
    }

    const emptyMsg = '<div style="text-align:center; padding: 20px; color: #666;">Usa el buscador o el escáner para agregar productos.</div>';
    document.getElementById('product-results').innerHTML = emptyMsg;

    document.getElementById('product-search').addEventListener('input', (e) => {
        const term = e.target.value.toLowerCase();
        const filtered = allProducts.filter(p =>
            p.name.toLowerCase().includes(term) ||
            (p.barcode && p.barcode.includes(term)) ||
            (p.item_number && p.item_number.toLowerCase().includes(term))
        );
        if (!term) {
            document.getElementById('product-results').innerHTML = emptyMsg;
        } else {
            renderProducts(filtered);
        }

        const exactMatch = allProducts.find(p => p.barcode === term || (p.item_number && p.item_number.toLowerCase() === term));
        if (exactMatch) {
            addToCart(exactMatch);
            e.target.value = '';
            document.getElementById('product-results').innerHTML = emptyMsg;
            document.getElementById('product-search').focus();
        }
    });

    const qtyInput = document.getElementById('pos-qty');
    if (qtyInput) {
        qtyInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                document.getElementById('product-search').focus();
            }
        });
    }

    document.getElementById('product-search').addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') return;
        e.preventDefault();
        const term = e.target.value.toLowerCase();
        if (!term) return;

        const exact = allProducts.find(p => p.barcode === term || (p.item_number && p.item_number.toLowerCase() === term));
        if (exact) {
            addToCart(exact);
            e.target.value = '';
            document.getElementById('product-results').innerHTML = emptyMsg;
            return;
        }

        const filtered = allProducts.filter(p =>
            p.name.toLowerCase().includes(term) ||
            (p.barcode && p.barcode.includes(term)) ||
            (p.item_number && p.item_number.toLowerCase().includes(term))
        );

        if (filtered.length > 0) {
            addToCart(filtered[0]);
            e.target.value = '';
            document.getElementById('product-results').innerHTML = emptyMsg;
        }
    });
});

function saveCartState() {
    try {
        const clientSelect = document.getElementById('client-select');
        const clientId = clientSelect ? clientSelect.value : '';
        localStorage.setItem('pos_cart_state', JSON.stringify({ cart, clientId }));
    } catch (e) {
        console.warn('No se pudo guardar el carrito:', e);
    }
}

function loadCartState() {
    try {
        const raw = localStorage.getItem('pos_cart_state');
        if (!raw) return;
        const state = JSON.parse(raw);
        if (state.cart) cart = state.cart;
        setTimeout(() => {
            const clientSelect = document.getElementById('client-select');
            if (clientSelect && state.clientId !== undefined) {
                clientSelect.value = state.clientId;
            }
            updateCart();
        }, 0);
    } catch (e) {
        console.warn('No se pudo cargar el carrito:', e);
    }
}

function renderProducts(products) {
    const container = document.getElementById('product-results');
    container.innerHTML = products.map(p => {
        const hasBulk = p.price_bulk && p.price_bulk > 0;
        const displayPrice = hasBulk ? p.price_bulk : p.price;

        return `
        <div style="cursor: pointer; padding: 12px; border: 1px solid rgba(0,0,0,0.1); border-radius: 8px; text-align: center; background: rgba(255,255,255,0.4); position: relative;">
            <button onclick="event.stopPropagation(); quickEditProduct(${p.id})"
                style="position: absolute; top: 4px; right: 4px; background: #2563eb; color: white; border: none; border-radius: 4px; padding: 4px 8px; cursor: pointer; font-size: 0.75rem; z-index: 10;">
                Editar
            </button>
            <div onclick='addToCart(${JSON.stringify(p)})'>
                <div style="font-weight: 600;">${p.name}</div>
                ${p.item_number ? `<div style="font-size: 0.8rem; color: #555; background: #eee; display: inline-block; padding: 2px 6px; border-radius: 4px; margin: 4px 0;">#${p.item_number}</div>` : ''}
                <div style="color: var(--primary-color); font-weight: 700;">
                    $${displayPrice}
                    ${hasBulk ? '<span style="font-size: 0.7rem; color: #b45309; display: block;">Precio bulto</span>' : ''}
                </div>
                <div style="font-size: 0.8rem; color: #666;">Stock: ${p.stock_quantity}</div>
            </div>
        </div>
        `;
    }).join('');
}

async function addToCart(product) {
    const prices = [
        { key: 'unit', label: 'Por unidad', val: product.price },
        { key: 'retail', label: 'Por mostrador', val: product.price_retail },
        { key: 'bulk', label: 'Por bulto', val: product.price_bulk }
    ];

    const inputOptions = {};
    const defaultKey = 'bulk';

    prices.forEach(p => {
        if (p.val && p.val > 0) {
            inputOptions[p.key] = `${p.label} ($${p.val})`;
        } else if (p.key === 'unit') {
            inputOptions[p.key] = `${p.label} ($${p.val || 0})`;
        }
    });

    const { value: selectedKey } = await Swal.fire({
        title: 'Seleccionar tarifa',
        text: product.name,
        input: 'radio',
        inputOptions,
        inputValue: inputOptions[defaultKey] ? defaultKey : 'unit',
        showCancelButton: true,
        confirmButtonText: 'Elegir cantidad',
        confirmButtonColor: '#2563eb',
        cancelButtonText: 'Cancelar'
    });

    if (!selectedKey) return;

    const finalPrice = prices.find(p => p.key === selectedKey).val;
    const finalLabel = prices.find(p => p.key === selectedKey).label;
    const lineKey = buildLineKey(product.id, selectedKey);

    const { value: qty } = await Swal.fire({
        title: 'Cantidad',
        html: `Producto: <b>${product.name}</b><br>Precio: <span style="color:green; font-weight:bold;">${finalLabel} ($${finalPrice})</span>`,
        input: 'number',
        inputValue: document.getElementById('pos-qty').value || 1,
        inputAttributes: { min: 1, step: 1 },
        showCancelButton: true,
        confirmButtonText: 'Agregar al carrito'
    });

    if (!qty || qty <= 0) return;

    const quantity = parseInt(qty, 10);
    const existing = cart.find(item => item.line_key === lineKey);
    if (existing) {
        existing.quantity += quantity;
    } else {
        cart.push({
            line_key: lineKey,
            price_key: selectedKey,
            product_id: product.id,
            product_name: product.name,
            item_number: product.item_number,
            unit_price: finalPrice,
            quantity,
            price_type: finalLabel
        });
    }

    document.getElementById('pos-qty').value = 1;
    document.getElementById('product-search').value = '';
    document.getElementById('product-search').focus();
    updateCart();
    saveCartState();

    Swal.fire({
        toast: true,
        position: 'top-end',
        icon: 'success',
        title: 'Agregado',
        showConfirmButton: false,
        timer: 1000
    });
}

function updateCart() {
    const tbody = document.getElementById('cart-body');
    let total = 0;

    tbody.innerHTML = cart.map(item => {
        const lineTotal = item.unit_price * item.quantity;
        total += lineTotal;
        return `
        <tr>
            <td>
                ${item.product_name}
                <div style="font-size: 0.75rem; color: #666;">
                    ${item.item_number ? `#${item.item_number} | ` : ''}
                    <span style="color: #2563eb; font-weight: bold;">${item.price_type}</span>
                </div>
            </td>
            <td>
                <div style="display: flex; align-items: center; gap: 4px;">
                    <button onclick="updateItemQty('${item.line_key}', -1)" style="width: 24px; height: 24px; border-radius: 4px; border: 1px solid #ccc; background: #eee; cursor: pointer;">-</button>
                    <span style="min-width: 20px; text-align: center;">${item.quantity}</span>
                    <button onclick="updateItemQty('${item.line_key}', 1)" style="width: 24px; height: 24px; border-radius: 4px; border: 1px solid #ccc; background: #eee; cursor: pointer;">+</button>
                </div>
            </td>
            <td>$${lineTotal.toFixed(2)}</td>
            <td><button onclick="removeFromCart('${item.line_key}')" style="background:none; border:none; color: red; cursor:pointer;">&times;</button></td>
        </tr>
        `;
    }).join('');

    document.getElementById('cart-total').innerText = '$' + total.toFixed(2);
    saveCartState();
}

function updateItemQty(lineKey, delta) {
    const item = cart.find(i => i.line_key === lineKey);
    if (!item) return;
    const newQty = item.quantity + delta;
    if (newQty > 0) {
        item.quantity = newQty;
        updateCart();
    }
}

function removeFromCart(lineKey) {
    cart = cart.filter(i => i.line_key !== lineKey);
    updateCart();
    saveCartState();
}

function checkout() {
    if (cart.length === 0) return alert('El carrito esta vacio');

    const clientSelect = document.getElementById('client-select');
    const clientName = clientSelect ? clientSelect.options[clientSelect.selectedIndex].text : 'Casual';
    const total = cart.reduce((acc, item) => acc + (item.unit_price * item.quantity), 0);

    document.getElementById('modal-total-display').textContent = '$' + total.toFixed(2);
    document.getElementById('modal-client-display').textContent = clientName;
    document.getElementById('payment-amount').value = total.toFixed(2);
    document.getElementById('payment-modal').style.display = 'flex';
    document.getElementById('payment-amount').focus();
    document.getElementById('payment-amount').select();
}

function closePaymentModal() {
    document.getElementById('payment-modal').style.display = 'none';
}

async function confirmCheckout() {
    const clientSelect = document.getElementById('client-select');
    const clientId = clientSelect ? clientSelect.value : null;
    const amountPaid = parseFloat(document.getElementById('payment-amount').value);
    const paymentMethod = document.getElementById('payment-method').value;

    if (isNaN(amountPaid) || amountPaid < 0) {
        return alert('Por favor ingrese un monto valido');
    }

    const salesData = {
        items: cart.map(i => ({ product_id: i.product_id, quantity: i.quantity })),
        client_id: clientId ? parseInt(clientId, 10) : null,
        amount_paid: amountPaid,
        payment_method: paymentMethod
    };

    const btn = document.querySelector('#payment-modal .btn');
    const originalText = btn.innerText;
    btn.disabled = true;
    btn.innerText = 'Procesando...';

    try {
        const res = await fetch('/api/sales', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(salesData)
        });

        if (res.ok) {
            const sale = await res.json();
            closePaymentModal();
            if (confirm('Venta realizada con exito. Desea generar el remito?')) {
                window.open(`/sales/${sale.id}/remito`, '_blank');
            }
            cart = [];
            updateCart();
            saveCartState();
            const pRes = await fetch('/api/products');
            allProducts = await pRes.json();
            document.getElementById('product-results').innerHTML = '<div style="text-align:center; padding: 20px; color: #666;">Usa el buscador o el escáner para agregar productos.</div>';
        } else {
            const err = await res.json();
            alert('Error: ' + err.detail);
        }
    } catch (e) {
        console.error(e);
        alert('Error de conexion o proceso: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.innerText = originalText;
    }
}

function handlePaymentMethodChange() {
    const method = document.getElementById('payment-method').value;
    const totalText = document.getElementById('modal-total-display').innerText.replace('$', '');
    const total = parseFloat(totalText);
    const amountInput = document.getElementById('payment-amount');
    amountInput.value = method === 'account' ? 0 : total.toFixed(2);
}

async function quickEditProduct(productId) {
    const product = allProducts.find(p => p.id === productId);
    if (!product) {
        Swal.fire('Error', 'Producto no encontrado', 'error');
        return;
    }

    const { value: formValues } = await Swal.fire({
        title: `Editar: ${product.name}`,
        html: `
            <div style="text-align: left;">
                <label style="font-weight: bold;">Precio unitario:</label>
                <input id="edit-price" type="number" step="0.01" value="${product.price || 0}" class="swal2-input" style="width: 90%;">
                <label style="font-weight: bold; margin-top: 10px; display: block;">Precio mostrador:</label>
                <input id="edit-price-retail" type="number" step="0.01" value="${product.price_retail || ''}" class="swal2-input" style="width: 90%;">
                <label style="font-weight: bold; margin-top: 10px; display: block;">Precio bulto:</label>
                <input id="edit-price-bulk" type="number" step="0.01" value="${product.price_bulk || ''}" class="swal2-input" style="width: 90%;">
                <label style="font-weight: bold; margin-top: 10px; display: block;">Stock:</label>
                <input id="edit-stock" type="number" value="${product.stock_quantity || 0}" class="swal2-input" style="width: 90%;">
            </div>
        `,
        focusConfirm: false,
        showCancelButton: true,
        confirmButtonText: 'Guardar',
        cancelButtonText: 'Cancelar',
        preConfirm: () => ({
            price: parseFloat(document.getElementById('edit-price').value),
            price_retail: parseFloat(document.getElementById('edit-price-retail').value) || null,
            price_bulk: parseFloat(document.getElementById('edit-price-bulk').value) || null,
            stock: parseInt(document.getElementById('edit-stock').value, 10)
        })
    });

    if (!formValues) return;

    try {
        const formData = new FormData();
        formData.append('name', product.name);
        formData.append('price', formValues.price);
        formData.append('stock', formValues.stock);
        formData.append('description', product.description || '');
        formData.append('barcode', product.barcode || '');
        formData.append('category', product.category || '');
        formData.append('item_number', product.item_number || '');
        formData.append('cant_bulto', product.cant_bulto || '');
        formData.append('numeracion', product.numeracion || '');
        if (formValues.price_retail) formData.append('price_retail', formValues.price_retail);
        if (formValues.price_bulk) formData.append('price_bulk', formValues.price_bulk);

        const res = await fetch(`/api/products/${productId}`, {
            method: 'PUT',
            body: formData
        });

        if (res.ok) {
            Swal.fire('Exito', 'Producto actualizado', 'success');
            const pRes = await fetch('/api/products');
            allProducts = await pRes.json();
            const term = document.getElementById('product-search').value.toLowerCase();
            const filtered = allProducts.filter(p =>
                p.name.toLowerCase().includes(term) ||
                (p.barcode && p.barcode.includes(term)) ||
                (p.item_number && p.item_number.toLowerCase().includes(term))
            );
            renderProducts(filtered);
        } else {
            Swal.fire('Error', 'No se pudo actualizar', 'error');
        }
    } catch (_e) {
        Swal.fire('Error', 'Fallo de conexion', 'error');
    }
}
