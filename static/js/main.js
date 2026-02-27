// ===========================================================
// SASP / SCIL 2025
// Script principal: interfaz, validaciones y peticiones AJAX
// ===========================================================

document.addEventListener("DOMContentLoaded", () => {

  // ===========================================================
  // DASHBOARD — Carga masiva de archivos Excel
  // ===========================================================
  const form = document.getElementById("uploadForm");
  if (form) {
    const input = document.getElementById("fileInput");
    const uploadArea = document.getElementById("uploadArea");
    const uploadStatus = document.getElementById("uploadStatus");
    const uploadResult = document.getElementById("uploadResult");
    const resultMessage = document.getElementById("resultMessage");

    uploadArea.addEventListener("click", () => input.click());
    uploadArea.addEventListener("dragover", (e) => {
      e.preventDefault();
      uploadArea.classList.add("dragging");
    });
    uploadArea.addEventListener("dragleave", () => {
      uploadArea.classList.remove("dragging");
    });
    uploadArea.addEventListener("drop", (e) => {
      e.preventDefault();
      input.files = e.dataTransfer.files;
      handleUpload(input.files);
    });
    input.addEventListener("change", () => handleUpload(input.files));

    async function handleUpload(files) {
      if (!files.length)
        return showMessage("Selecciona al menos un archivo Excel válido (.xlsx o .xls)", true);

      const formData = new FormData();
      for (const file of files) {
        if (!/\.(xlsx|xls)$/i.test(file.name))
          return showMessage(`Archivo no válido: ${file.name}`, true);
        formData.append("files", file);
      }

      uploadStatus.hidden = false;
      uploadArea.style.display = "none";
      uploadResult.hidden = true;

      try {
        const res = await fetch(form.action, {
          method: "POST",
          body: formData,
          credentials: "same-origin",
          headers: {
            'X-Requested-With': 'XMLHttpRequest'
          }
        });

        uploadStatus.hidden = true;
        uploadArea.style.display = "block";

        const contentType = res.headers.get("content-type");
        let data = null;
        if (contentType && contentType.includes("application/json")) {
          data = await res.json();
        }

        if (!res.ok) {
          if (data && data.error) throw new Error(data.error);
          if (res.status === 401 || res.status === 403) {
            throw new Error("Sesión expirada o no autorizada. Por favor, inicia sesión nuevamente.");
          }
          if (res.status === 413) {
            throw new Error("Los archivos exceden el tamaño máximo permitido.");
          }
          throw new Error(`Error del servidor (${res.status})`);
        }

        if (data && data.error) throw new Error(data.error);
        if (!data) throw new Error("Respuesta inválida del servidor.");

        // Mostrar alertas si existen
        let alertasHTML = '';
        if (data.alertas && data.alertas.length > 0) {
          alertasHTML = '<div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 1rem; margin-top: 1rem; border-radius: 6px;">';
          alertasHTML += '<strong style="color: #92400e;">⚠️ Advertencias:</strong><ul style="margin: 0.5rem 0 0 1.2rem; color: #92400e;">';
          data.alertas.forEach(alerta => {
            alertasHTML += `<li>${alerta.mensaje}</li>`;
          });
          alertasHTML += '</ul></div>';
        }

        uploadResult.hidden = false;
        resultMessage.innerHTML = `
          <strong>${data.mensaje || "Procesamiento completado"}</strong><br>
          <span>${data.total_procesados || data.total_resultados || 0} registros procesados.</span><br>
          <span>${data.insertados || data.nuevos || 0} nuevos registros guardados.</span><br>
          <span>${data.actualizados || 0} registros actualizados.</span>
          ${alertasHTML}
        `;
      } catch (err) {
        uploadStatus.hidden = true;
        uploadArea.style.display = "block";
        showMessage("❌ Error al procesar los archivos: " + err.message, true);
      }
    }

    function showMessage(text, isError = false) {
      const msg = document.createElement("div");
      msg.className = `upload-message ${isError ? "error" : "success"}`;
      msg.innerHTML = text;
      form.after(msg);
      setTimeout(() => msg.remove(), 5000);
    }
  }

  // ===========================================================
  // RESULTADOS — Búsqueda en tiempo real y exportaciones
  // ===========================================================
  // Accordion toggle functionality
  const acordeonHeaders = document.querySelectorAll(".acordeon-header");
  acordeonHeaders.forEach(header => {
    header.addEventListener("click", () => {
      const acordeon = header.closest(".ente-bloque.acordeon");
      const contenido = acordeon.querySelector(".acordeon-contenido");
      const icono = header.querySelector(".acordeon-icono");

      if (contenido.style.display === "block") {
        contenido.style.display = "none";
        if (icono) icono.textContent = "▶";
      } else {
        contenido.style.display = "block";
        if (icono) icono.textContent = "▼";
      }
    });
  });

  // Search functionality with accordion integration
  function normalizeSearchText(text) {
    return (text || "")
      .toString()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/\s+/g, " ")
      .trim();
  }

  const searchInput = document.getElementById("searchInput");
  if (searchInput) {
    const runSearch = () => {
      const query = normalizeSearchText(searchInput.value);
      const terms = query ? query.split(" ") : [];
      const acordeones = document.querySelectorAll(".ente-bloque.acordeon");

      acordeones.forEach(acordeon => {
        const filas = acordeon.querySelectorAll("tr.search-row");
        let hasVisibleRows = false;

        filas.forEach(fila => {
          const textoBusqueda = normalizeSearchText(fila.dataset.search || "");
          const matches = terms.length === 0 || terms.every(term => textoBusqueda.includes(term));

          if (matches) {
            fila.style.display = "";
            hasVisibleRows = true;
          } else {
            fila.style.display = "none";
          }
        });

        // Show/hide accordion based on whether it has visible rows
        if (query && filas.length > 0 && !hasVisibleRows) {
          acordeon.style.display = "none";
        } else {
          acordeon.style.display = "";
          // Auto-expand if search has results
          if (query && hasVisibleRows) {
            const contenido = acordeon.querySelector(".acordeon-contenido");
            const icono = acordeon.querySelector(".acordeon-icono");
            if (contenido) contenido.style.display = "block";
            if (icono) icono.textContent = "▼";
          } else if (!query) {
            const icono = acordeon.querySelector(".acordeon-icono");
            if (icono) icono.textContent = "▶";
          }
        }
      });
    };

    searchInput.addEventListener("input", runSearch);
    searchInput.addEventListener("search", runSearch);
  }

  const selectEnte = document.getElementById("selectEnte");
  if (selectEnte) {
    const exportForm = selectEnte.closest("form");
    exportForm.addEventListener("submit", (e) => {
      if (!selectEnte.value.trim()) {
        e.preventDefault();
        alert("Selecciona un ente antes de exportar.");
      }
    });
  }

  // Pre-validación de duplicados (solo para Luis)
  const preForms = document.querySelectorAll(".prevalidacion-form");
  preForms.forEach(form => {
    const estadoEl = form.querySelector(".pre-estado");
    const catalogoWrap = form.querySelector(".pre-catalogo-wrap");
    const catalogoEl = form.querySelector(".pre-catalogo");
    const otroWrap = form.querySelector(".pre-otro-wrap");
    const otroEl = form.querySelector(".pre-otro-texto");

    function syncPreFormVisibility() {
      const estado = estadoEl ? estadoEl.value : "Sin valoración";
      const muestraCatalogo = estado === "Solventado";
      if (catalogoWrap) catalogoWrap.style.display = muestraCatalogo ? "" : "none";

      const muestraOtro = muestraCatalogo && catalogoEl && catalogoEl.value === "Otro";
      if (otroWrap) otroWrap.style.display = muestraOtro ? "" : "none";

      if (!muestraCatalogo && catalogoEl) {
        catalogoEl.value = "";
      }
      if (!muestraOtro && otroEl) {
        otroEl.value = "";
      }
    }

    if (estadoEl) {
      estadoEl.addEventListener("change", () => {
        syncPreFormVisibility();
        if (estadoEl.value === "Sin valoración") {
          form.requestSubmit();
        }
      });
    }
    if (catalogoEl) {
      catalogoEl.addEventListener("change", () => {
        syncPreFormVisibility();
        if (estadoEl && estadoEl.value === "Solventado" && catalogoEl.value && catalogoEl.value !== "Otro") {
          form.requestSubmit();
        }
      });
    }
    if (otroEl) {
      otroEl.addEventListener("blur", () => {
        if (
          estadoEl &&
          catalogoEl &&
          estadoEl.value === "Solventado" &&
          catalogoEl.value === "Otro" &&
          otroEl.value.trim()
        ) {
          form.requestSubmit();
        }
      });
    }
    syncPreFormVisibility();

    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      if (form.dataset.saving === "1") {
        return;
      }

      const rfc = form.dataset.rfc || "";
      const ente = form.dataset.ente || "";
      const msg = form.querySelector(".prevalidacion-msg");
      const estado = form.querySelector('select[name="pre_estado"]')?.value || "Sin valoración";
      const catalogo = form.querySelector('select[name="pre_catalogo"]')?.value || "";
      const otroTexto = form.querySelector('textarea[name="pre_otro_texto"]')?.value?.trim() || "";

      if (!rfc || !ente) {
        if (msg) msg.textContent = "Faltan datos RFC/ente.";
        return;
      }

      if (estado === "Solventado" && !catalogo) {
        if (msg) msg.textContent = "Selecciona una opción de catálogo.";
        return;
      }
      if (catalogo === "Otro" && !otroTexto) {
        if (msg) msg.textContent = "Escribe el texto para la opción Otro.";
        return;
      }

      form.dataset.saving = "1";
      if (msg) msg.textContent = "Guardando...";

      try {
        const res = await fetch("/prevalidar_duplicado", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            rfc,
            ente,
            estado,
            catalogo,
            otro_texto: otroTexto
          })
        });

        const contentType = res.headers.get("content-type");
        if (!contentType || !contentType.includes("application/json")) {
          throw new Error("Respuesta inválida del servidor");
        }

        const data = await res.json();
        if (!res.ok || data.error) {
          throw new Error(data.error || `Error ${res.status}`);
        }

        if (msg) msg.textContent = "✓ Pre-validación guardada";
      } catch (error) {
        if (msg) msg.textContent = "✗ " + error.message;
      } finally {
        form.dataset.saving = "0";
      }
    });
  });

  // ===========================================================
  // CATÁLOGOS — Pestañas dinámicas
  // ===========================================================
  const tabs = document.querySelectorAll(".tab");
  const contents = document.querySelectorAll(".tab-content");
  if (tabs.length) {
    tabs.forEach(tab => {
      tab.addEventListener("click", () => {
        tabs.forEach(t => t.classList.remove("active"));
        contents.forEach(c => c.classList.remove("active"));
        tab.classList.add("active");
        document.getElementById("tab-" + tab.dataset.tab).classList.add("active");
      });
    });
  }

  // ===========================================================
  // DETALLE RFC — Indicador de carga en botones
  // ===========================================================
  const btns = document.querySelectorAll(".btn");
  btns.forEach(btn => {
    btn.addEventListener("click", () => {
      btn.classList.add("clicked");
      setTimeout(() => btn.classList.remove("clicked"), 800);
    });
  });


	// ===========================================================
// SOLVENTACIÓN — Envío asíncrono
// ===========================================================
const formSolv = document.getElementById("solventacionForm");
if (formSolv) {
  formSolv.addEventListener("submit", async (e) => {
    e.preventDefault();
    const rfc = formSolv.dataset.rfc;
    const estado = document.getElementById("estado").value;
    const valoracionEl = document.getElementById("valoracion");
    const valoracion = valoracionEl ? valoracionEl.value.trim() : "";
    const catalogo = document.getElementById("catalogo").value;
    const otroTexto = document.getElementById("otro_texto").value.trim();
    const ente = document.querySelector('input[name="ente"]')?.value || null;
    const confirmacion = document.getElementById("confirmacion");

    if (!estado) {
      return setMsg("Selecciona un estado antes de guardar.", true);
    }

    // Validar que si el estado es Solventado o No Solventado, el catálogo sea obligatorio
    if ((estado === "Solventado" || estado === "No Solventado") && !catalogo) {
      return setMsg("Debes seleccionar una opción del Catálogo de Soluciones.", true);
    }

    // Validar que si el catálogo es "Otro", el campo otro_texto sea obligatorio
    if (catalogo === "Otro" && !otroTexto) {
      return setMsg("Debes especificar la solución cuando seleccionas 'Otro'.", true);
    }

    confirmacion.style.display = "block";
    confirmacion.textContent = "Guardando...";
    confirmacion.className = "confirmacion";

    try {
      const res = await fetch("/actualizar_estado", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rfc, estado, valoracion, catalogo, otro_texto: otroTexto, ente })
      });

      // Check if response is JSON before parsing
      const contentType = res.headers.get("content-type");
      if (!contentType || !contentType.includes("application/json")) {
        throw new Error("Sesión expirada o no autorizada. Por favor, inicia sesión nuevamente.");
      }

      const data = await res.json();

      if (!res.ok || data.error)
        throw new Error(data.error || `Error del servidor (${res.status})`);

      setMsg("✅ " + (data.mensaje || "Registro actualizado correctamente."), false);
      setTimeout(() => window.location.href = `/resultados/${rfc}`, 1500);

    } catch (err) {
      setMsg("❌ Error: " + err.message, true);
    }

    function setMsg(msg, error) {
      confirmacion.textContent = msg;
      confirmacion.className = "confirmacion " + (error ? "error" : "ok");
      confirmacion.style.display = "block";
    }
  });
}

  // ===========================================================
  // EMPTY PAGE — Acción de volver a resultados
  // ===========================================================
  const btnVolver = document.querySelector(".empty-container .btn-primary");
  if (btnVolver) {
    btnVolver.addEventListener("click", () => {
      btnVolver.classList.add("clicked");
      setTimeout(() => btnVolver.classList.remove("clicked"), 500);
    });
  }
});
