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
    const selectedFilesList = document.getElementById("selectedFilesList");
    const selectedFilesEmpty = document.getElementById("selectedFilesEmpty");
    const uploadStatus = document.getElementById("uploadStatus");
    const uploadResult = document.getElementById("uploadResult");
    const resultStatusTitle = document.getElementById("resultStatusTitle");
    const resultMessage = document.getElementById("resultMessage");
    const processUploadBtn = document.getElementById("processUploadBtn");
    const replaceQuincenasBtn = document.getElementById("replaceQuincenasBtn");
    const quincenasProgressLabel = document.getElementById("quincenasProgressLabel");
    const quincenasProgressText = document.getElementById("quincenasProgressText");
    const quincenasProgressPercent = document.getElementById("quincenasProgressPercent");
    const quincenasProgressBar = document.getElementById("quincenasProgressBar");

    const horariosForm = document.getElementById("uploadHorariosForm");
    const horariosInput = document.getElementById("horariosFileInput");
    const uploadHorariosArea = document.getElementById("uploadHorariosArea");
    const horariosFilesList = document.getElementById("horariosFilesList");
    const horariosFilesEmpty = document.getElementById("horariosFilesEmpty");
    const uploadHorariosStatus = document.getElementById("uploadHorariosStatus");
    const uploadHorariosResult = document.getElementById("uploadHorariosResult");
    const horariosResultTitle = document.getElementById("horariosResultTitle");
    const horariosResultMessage = document.getElementById("horariosResultMessage");
    const processHorariosUploadBtn = document.getElementById("processHorariosUploadBtn");
    const replaceHorariosBtn = document.getElementById("replaceHorariosBtn");
    const horariosProgressLabel = document.getElementById("horariosProgressLabel");
    const horariosProgressText = document.getElementById("horariosProgressText");
    const horariosProgressPercent = document.getElementById("horariosProgressPercent");
    const horariosProgressBar = document.getElementById("horariosProgressBar");

    const drawerNode = document.getElementById("dashboardProcessedDrawerData");
    const processedDrawerToggle = document.getElementById("processedDrawerToggle");
    const processedDrawerOverlay = document.getElementById("processedDrawerOverlay");
    const processedDrawer = document.getElementById("processedDrawer");
    const processedDrawerClose = document.getElementById("processedDrawerClose");
    const processedDrawerCount = document.getElementById("processedDrawerCount");
    const processedDrawerMeta = document.getElementById("processedDrawerMeta");
    const processedTableBody = document.getElementById("processedTableBody");
    const processedEmpty = document.getElementById("processedEmpty");
    const processedFilterButtons = Array.from(document.querySelectorAll("[data-processed-filter]"));

    let selectedFiles = [];
    let selectedHorariosFiles = [];
    let isProcessingUpload = false;
    let isProcessingHorariosUpload = false;
    let activeProcessedFilter = "trabajadores";
    let processedSections = parseDrawerData(drawerNode);
    const fileStates = new Map();
    const horarioFileStates = new Map();

    const STATUS_META = {
      ready: { label: "Listo", tone: "success" },
      invalid: { label: "Inválido", tone: "error" },
      processing: { label: "Procesando", tone: "info" },
      uploaded: { label: "Cargado", tone: "success" },
      warning: { label: "Con alertas", tone: "warning" },
      failed: { label: "Error", tone: "error" },
    };

    const UPLOAD_STEPS = {
      quincenas: [
        { progress: 18, label: "Subiendo archivo", text: "Transferencia en curso." },
        { progress: 52, label: "Validando datos", text: "Revisando estructura y contenido." },
        { progress: 84, label: "Procesando quincena", text: "Actualizando registros operativos." },
      ],
      horarios: [
        { progress: 18, label: "Subiendo archivo", text: "Transferencia en curso." },
        { progress: 52, label: "Validando datos", text: "Revisando formato y vigencias." },
        { progress: 84, label: "Procesando horarios", text: "Guardando horarios en la base." },
      ],
    };

    function parseDrawerData(node) {
      if (!node) {
        return { trabajadores: [], horarios: [] };
      }
      try {
        const data = JSON.parse(node.textContent || "{}");
        return {
          trabajadores: Array.isArray(data.trabajadores) ? data.trabajadores : [],
          horarios: Array.isArray(data.horarios) ? data.horarios : [],
        };
      } catch (_error) {
        return { trabajadores: [], horarios: [] };
      }
    }

    function getFileKey(file) {
      return [file.name, file.size, file.lastModified].join("::");
    }

    function isExcelFile(file) {
      return /\.(xlsx|xls)$/i.test(file.name || "");
    }

    function formatFileSize(bytes) {
      const size = Number(bytes || 0);
      if (!size) return "0 KB";
      if (size < 1024) return `${size} B`;
      if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
      return `${(size / (1024 * 1024)).toFixed(1)} MB`;
    }

    function syncInputFiles(files, targetInput) {
      const dataTransfer = new DataTransfer();
      files.forEach((file) => dataTransfer.items.add(file));
      targetInput.files = dataTransfer.files;
    }

    function bindDropzone(zone, targetInput, handler) {
      if (!zone || !targetInput) return;
      zone.addEventListener("click", () => targetInput.click());
      zone.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          targetInput.click();
        }
      });
      zone.addEventListener("dragover", (event) => {
        event.preventDefault();
        zone.classList.add("dragging");
      });
      zone.addEventListener("dragleave", () => {
        zone.classList.remove("dragging");
      });
      zone.addEventListener("drop", (event) => {
        event.preventDefault();
        zone.classList.remove("dragging");
        handler(Array.from(event.dataTransfer.files || []));
      });
    }

    function mergeSelectedFiles(newFiles, target, stateMap, renderFn, onlyOne = false) {
      if (!newFiles.length) return target;
      const files = onlyOne ? [newFiles[newFiles.length - 1]] : [...target];
      const existingKeys = new Set(files.map((file) => getFileKey(file)));
      newFiles.forEach((file) => {
        const fileKey = getFileKey(file);
        if (onlyOne) {
          files.length = 0;
          stateMap.clear();
        }
        if (existingKeys.has(fileKey) && !onlyOne) return;
        existingKeys.add(fileKey);
        files.push(file);
        stateMap.set(fileKey, isExcelFile(file) ? "ready" : "invalid");
      });
      renderFn(files);
      return files;
    }

    function renderFileList(listNode, emptyNode, files, stateMap, removeAttr, processBtn, isBusy) {
      if (!listNode || !emptyNode) return;
      if (!files.length) {
        listNode.hidden = true;
        listNode.innerHTML = "";
        emptyNode.hidden = false;
        if (processBtn) processBtn.disabled = true;
        return;
      }

      emptyNode.hidden = true;
      listNode.hidden = false;
      listNode.innerHTML = files.map((file) => {
        const statusMeta = STATUS_META[stateMap.get(getFileKey(file)) || "ready"] || STATUS_META.ready;
        return `
          <li class="upload-file-item">
            <div class="upload-file-copy">
              <strong>${escapeHtml(file.name)}</strong>
              <span>${formatFileSize(file.size)} · ${escapeHtml((file.name.split(".").pop() || "").toUpperCase())}</span>
            </div>
            <div class="upload-file-actions">
              <span class="file-status-badge tone-${statusMeta.tone}">${statusMeta.label}</span>
              <button type="button" class="file-remove-button" ${removeAttr}="${escapeHtml(getFileKey(file))}" aria-label="Quitar ${escapeHtml(file.name)}">Quitar</button>
            </div>
          </li>
        `;
      }).join("");

      if (processBtn) {
        processBtn.disabled = isBusy || !files.some((file) => isExcelFile(file));
      }
    }

    function setProgressState(labelNode, textNode, percentNode, barNode, statusNode, label, text, progress) {
      if (statusNode) statusNode.hidden = false;
      if (labelNode) labelNode.textContent = label;
      if (textNode) textNode.textContent = text;
      if (percentNode) percentNode.textContent = `${progress}%`;
      if (barNode) barNode.style.width = `${progress}%`;
    }

    function setProcessedDrawerOpen(isOpen) {
      if (!processedDrawer || !processedDrawerOverlay || !processedDrawerToggle) return;
      processedDrawer.classList.toggle("is-open", isOpen);
      processedDrawerOverlay.classList.toggle("is-visible", isOpen);
      processedDrawer.setAttribute("aria-hidden", isOpen ? "false" : "true");
      processedDrawerToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
    }

    function renderProcessedFiles() {
      if (!processedTableBody || !processedEmpty) return;
      const currentItems = Array.isArray(processedSections[activeProcessedFilter]) ? processedSections[activeProcessedFilter] : [];
      const totalItems = (processedSections.trabajadores || []).length + (processedSections.horarios || []).length;

      if (processedDrawerCount) processedDrawerCount.textContent = String(totalItems);
      if (processedDrawerMeta) {
        processedDrawerMeta.textContent = activeProcessedFilter === "horarios"
          ? "Archivos procesados de horarios"
          : "Archivos procesados de trabajadores";
      }

      processedFilterButtons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.processedFilter === activeProcessedFilter);
      });

      if (!currentItems.length) {
        processedTableBody.innerHTML = "";
        processedEmpty.style.display = "block";
        return;
      }

      processedEmpty.style.display = "none";
      processedTableBody.innerHTML = currentItems.map((item) => `
        <article class="processed-item">
          <div class="processed-item-copy">
            <strong>${escapeHtml(item.label || "Archivo sin nombre")}</strong>
            <span>${escapeHtml(item.secondary || item.tipo || "")}</span>
          </div>
          <div class="processed-item-meta">
            <small>${escapeHtml(item.fecha || "Sin fecha")}</small>
            ${item.href ? `<a href="${escapeHtml(item.href)}" class="processed-link">${escapeHtml(item.cta || "Abrir")}</a>` : ""}
          </div>
        </article>
      `).join("");
    }

    async function refreshProcessedFiles() {
      try {
        const response = await fetch("/api/dashboard/archivos-procesados", {
          credentials: "same-origin",
          headers: { "X-Requested-With": "XMLHttpRequest" },
        });
        const data = await response.json();
        if (!response.ok || data.error) {
          throw new Error((data && data.error) || `Error ${response.status}`);
        }
        processedSections = {
          trabajadores: Array.isArray(data.secciones?.trabajadores) ? data.secciones.trabajadores : [],
          horarios: Array.isArray(data.secciones?.horarios) ? data.secciones.horarios : [],
        };
        renderProcessedFiles();
      } catch (error) {
        showMessage(`No fue posible actualizar archivos procesados: ${error.message}`, true);
      }
    }

    async function processFiles({ files, action, stateMap, renderFn, progressNodes, resultNodes, areaNode, kind }) {
      const validFiles = files.filter((file) => isExcelFile(file));
      if (!validFiles.length) {
        showMessage("Selecciona un archivo Excel válido para procesar.", true);
        return;
      }

      const formData = new FormData();
      validFiles.forEach((file) => {
        formData.append("files", file);
        stateMap.set(getFileKey(file), "processing");
      });
      renderFn(files);

      const steps = UPLOAD_STEPS[kind];
      const [labelNode, textNode, percentNode, barNode, statusNode] = progressNodes;
      const [resultWrap, resultTitleNode, resultMessageNode] = resultNodes;

      if (resultWrap) resultWrap.hidden = true;
      if (areaNode) areaNode.classList.add("is-processing");
      setProgressState(labelNode, textNode, percentNode, barNode, statusNode, steps[0].label, steps[0].text, steps[0].progress);

      const timeouts = [
        window.setTimeout(() => setProgressState(labelNode, textNode, percentNode, barNode, statusNode, steps[1].label, steps[1].text, steps[1].progress), 220),
        window.setTimeout(() => setProgressState(labelNode, textNode, percentNode, barNode, statusNode, steps[2].label, steps[2].text, steps[2].progress), 650),
      ];

      try {
        const response = await fetch(action, {
          method: "POST",
          body: formData,
          credentials: "same-origin",
          headers: { "X-Requested-With": "XMLHttpRequest" },
        });
        const data = await response.json();
        if (!response.ok || data.error) {
          throw new Error((data && data.error) || `Error ${response.status}`);
        }

        validFiles.forEach((file) => {
          stateMap.set(getFileKey(file), data.alertas && data.alertas.length ? "warning" : "uploaded");
        });
        renderFn(files);
        setProgressState(labelNode, textNode, percentNode, barNode, statusNode, "Completado", data.mensaje || "Archivo procesado.", 100);
        if (resultTitleNode) {
          resultTitleNode.textContent = data.alertas && data.alertas.length ? "Procesado con alertas" : "Procesado correctamente";
        }
        if (resultMessageNode) {
          const detail = kind === "quincenas"
            ? `${Number(data.total_procesados || 0)} registros procesados`
            : `${Number(data.guardados_total || 0)} horarios guardados`;
          resultMessageNode.textContent = `${data.mensaje || "Carga completada."} ${detail}.`;
        }
        if (resultWrap) resultWrap.hidden = false;
        refreshProcessedFiles();
        showMessage(data.mensaje || "Carga completada.", false);
      } catch (error) {
        validFiles.forEach((file) => {
          stateMap.set(getFileKey(file), "failed");
        });
        renderFn(files);
        setProgressState(labelNode, textNode, percentNode, barNode, statusNode, "Error", error.message, 100);
        if (resultTitleNode) resultTitleNode.textContent = "No se pudo procesar";
        if (resultMessageNode) resultMessageNode.textContent = error.message;
        if (resultWrap) resultWrap.hidden = false;
        showMessage(`Error al procesar archivo: ${error.message}`, true);
      } finally {
        timeouts.forEach((timeoutId) => window.clearTimeout(timeoutId));
        if (areaNode) areaNode.classList.remove("is-processing");
      }
    }

    function escapeHtml(value) {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function showMessage(text, isError = false) {
      const msg = document.createElement("div");
      msg.className = `upload-message ${isError ? "error" : "success"}`;
      msg.textContent = text;
      form.after(msg);
      setTimeout(() => msg.remove(), 5000);
    }

    function renderQuincenas(files = selectedFiles) {
      renderFileList(
        selectedFilesList,
        selectedFilesEmpty,
        files,
        fileStates,
        "data-remove-file",
        processUploadBtn,
        isProcessingUpload
      );
    }

    function renderHorarios(files = selectedHorariosFiles) {
      renderFileList(
        horariosFilesList,
        horariosFilesEmpty,
        files,
        horarioFileStates,
        "data-remove-horario-file",
        processHorariosUploadBtn,
        isProcessingHorariosUpload
      );
    }

    bindDropzone(uploadArea, input, (files) => {
      selectedFiles = mergeSelectedFiles(files, selectedFiles, fileStates, renderQuincenas);
      syncInputFiles(selectedFiles, input);
      uploadResult.hidden = true;
    });
    bindDropzone(uploadHorariosArea, horariosInput, (files) => {
      selectedHorariosFiles = mergeSelectedFiles(files, selectedHorariosFiles, horarioFileStates, renderHorarios, true);
      syncInputFiles(selectedHorariosFiles, horariosInput);
      uploadHorariosResult.hidden = true;
    });

    input.addEventListener("change", () => {
      selectedFiles = mergeSelectedFiles(Array.from(input.files || []), selectedFiles, fileStates, renderQuincenas);
      syncInputFiles(selectedFiles, input);
      input.value = "";
      uploadResult.hidden = true;
    });

    if (horariosInput) {
      horariosInput.addEventListener("change", () => {
        selectedHorariosFiles = mergeSelectedFiles(Array.from(horariosInput.files || []), selectedHorariosFiles, horarioFileStates, renderHorarios, true);
        syncInputFiles(selectedHorariosFiles, horariosInput);
        horariosInput.value = "";
        uploadHorariosResult.hidden = true;
      });
    }

    if (replaceQuincenasBtn) {
      replaceQuincenasBtn.addEventListener("click", () => input.click());
    }
    if (replaceHorariosBtn) {
      replaceHorariosBtn.addEventListener("click", () => horariosInput.click());
    }

    if (selectedFilesList) {
      selectedFilesList.addEventListener("click", (event) => {
        const button = event.target.closest("[data-remove-file]");
        if (!button) return;
        const key = button.getAttribute("data-remove-file");
        selectedFiles = selectedFiles.filter((file) => getFileKey(file) !== key);
        fileStates.delete(key);
        syncInputFiles(selectedFiles, input);
        renderQuincenas();
      });
    }

    if (horariosFilesList) {
      horariosFilesList.addEventListener("click", (event) => {
        const button = event.target.closest("[data-remove-horario-file]");
        if (!button) return;
        const key = button.getAttribute("data-remove-horario-file");
        selectedHorariosFiles = selectedHorariosFiles.filter((file) => getFileKey(file) !== key);
        horarioFileStates.delete(key);
        syncInputFiles(selectedHorariosFiles, horariosInput);
        renderHorarios();
      });
    }

    if (processUploadBtn) {
      processUploadBtn.addEventListener("click", async () => {
        isProcessingUpload = true;
        processUploadBtn.disabled = true;
        await processFiles({
          files: selectedFiles,
          action: form.action,
          stateMap: fileStates,
          renderFn: renderQuincenas,
          progressNodes: [quincenasProgressLabel, quincenasProgressText, quincenasProgressPercent, quincenasProgressBar, uploadStatus],
          resultNodes: [uploadResult, resultStatusTitle, resultMessage],
          areaNode: uploadArea,
          kind: "quincenas",
        });
        isProcessingUpload = false;
        renderQuincenas();
      });
    }

    if (processHorariosUploadBtn) {
      processHorariosUploadBtn.addEventListener("click", async () => {
        isProcessingHorariosUpload = true;
        processHorariosUploadBtn.disabled = true;
        await processFiles({
          files: selectedHorariosFiles,
          action: horariosForm.action,
          stateMap: horarioFileStates,
          renderFn: renderHorarios,
          progressNodes: [horariosProgressLabel, horariosProgressText, horariosProgressPercent, horariosProgressBar, uploadHorariosStatus],
          resultNodes: [uploadHorariosResult, horariosResultTitle, horariosResultMessage],
          areaNode: uploadHorariosArea,
          kind: "horarios",
        });
        isProcessingHorariosUpload = false;
        renderHorarios();
      });
    }

    if (processedDrawerToggle) {
      processedDrawerToggle.addEventListener("click", () => setProcessedDrawerOpen(true));
    }
    if (processedDrawerClose) {
      processedDrawerClose.addEventListener("click", () => setProcessedDrawerOpen(false));
    }
    if (processedDrawerOverlay) {
      processedDrawerOverlay.addEventListener("click", () => setProcessedDrawerOpen(false));
    }
    processedFilterButtons.forEach((button) => {
      button.addEventListener("click", () => {
        activeProcessedFilter = button.dataset.processedFilter || "trabajadores";
        renderProcessedFiles();
      });
    });

    renderQuincenas();
    renderHorarios();
    renderProcessedFiles();
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
