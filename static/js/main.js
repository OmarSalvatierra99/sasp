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
        const res = await fetch(form.action, { method: "POST", body: formData });
        const data = await res.json();

        uploadStatus.hidden = true;
        uploadArea.style.display = "block";

        if (!res.ok || data.error)
          throw new Error(data.error || `Error del servidor (${res.status})`);

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
          <span>${data.total_resultados || 0} registros detectados.</span><br>
          <span>${data.nuevos || 0} nuevos registros guardados.</span>
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
  // RESULTADOS — Exportaciones y validaciones simples
  // ===========================================================
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
    const valoracion = document.getElementById("valoracion").value.trim();
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

