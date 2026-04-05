import argparse
import datetime as dt
import json
import os
import random
import re
import shutil
import unicodedata
from pathlib import Path

import fitz  # PyMuPDF
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError

PDF_DIRECTORY = "downloaded_pdfs"
PROCESSED_PDFS_FILE = "vec_processed_pdfs.json"
SKIPPED_PDFS_FILE = "vec_skipped_pdfs.json"
SKIPPED_PDF_ARCHIVE_DIR = Path("skipped_pdfs")
OUTPUT_JSON_FILE = "classification_result.json"
OUTPUT_TEXT_FILE = "classification_source_text.txt"
MODEL_NAME = "gpt-4.1-mini"
OFFICIAL_VEC_MANUAL_FILENAME = "MANUAL_OPERATIVO_VEC_SAS_OFICIAL_V1.pdf"
OFFICIAL_VEC_CENTER_CODES_FILENAME = "CODIGOS_CENTROS_VEC_SAS_OFICIAL - Todos.pdf"
DEBUG_DIR = Path("vec_debug")
DEBUG_DIR.mkdir(exist_ok=True)
OPENAI_REQUEST_PREVIEW_FILE = DEBUG_DIR / "openai_request_preview.json"
OPENAI_UPLOADED_FILE_IDS = {}
OFFICIAL_VEC_SOURCE_SUMMARY = """
Verified local official references:
- MANUAL_OPERATIVO_VEC_SAS_OFICIAL_V1.pdf
  - Formacion continuada fields in VEC: fecha inicio, fecha fin, codigo del centro, nombre del merito, valores especificos, seleccion del documento, firma.
  - Formacion continuada methodology rule: if the document does not clearly indicate presencial or semipresencial, select A distancia.
  - Formacion continuada accreditation rule: if accredited, the accrediting body to select is Comision de Formacion Continuada del SNS.
  - Credits/hours rule: if CFC credits appear, capture credits; otherwise capture hours. Never convert hours into credits and never invent credits.
  - Critical control rule: the attached document name must not exceed 50 characters and the attached description must be brief.
  - Diplomas y titulos propios universitarios must be used for non-official university training; do not confuse them with master oficial.
  - Experiencia SAS is not uploaded manually; it must use the load merits / cargar de oficio flow.
  - Every experience contract period must be uploaded independently, without overlaps and without filling gaps.
  - Vida laboral is mandatory for otros centros sanitarios, otros centros no sanitarios, experiencia extracomunitaria, and otras administraciones publicas.
- CODIGOS_CENTROS_VEC_SAS_OFICIAL - Todos.pdf
  - Use the official center catalog labels and names for center lookup.
  - Official examples present in the catalog include: 11-CENTROS O ENTIDADES SANITARIAS SERVICIO ANDALUZ DE SALUD, 12-CENTROS O ENTIDADES SANITARIAS SNS, 24-UNIVERSIDAD ESPANOLA O UE, 13-COLEGIO OFICIAL, 02-Asociacion/Federacion.
"""

CLIENT_GPT_INSTRUCTIONS = (
    """
IDENTIDAD DEL SISTEMA

Eres el Asistente Experto Operativo de la Ventanilla Electronica del Candidato (VEC) del Servicio Andaluz de Salud (SAS).
Tu funcion es clasificar meritos y devolver un unico JSON valido para la automatizacion VEC.
Actuas como sistema operativo administrativo especializado en la VEC.
No simplificas procesos. No omites desplegables. No inventas datos.

OBJETIVO

- Clasificar correctamente cada merito dentro del catalogo oficial.
- Indicar la ruta exacta dentro de la VEC usando vec_route.
- Extraer solo los datos soportados por el PDF.
- Detectar errores que puedan provocar exclusion o no puntuacion.
- Indicar documentacion obligatoria adicional en required_documents.
- Reflejar alertas y riesgos en alerts.

FUENTES OBLIGATORIAS Y PRIORIDAD

1. Manual Operativo
2. Archivo de Codigos de Centros
3. Reglas globales
4. Conocimiento general

Resumen verificado de fuentes oficiales locales:
"""
    + OFFICIAL_VEC_SOURCE_SUMMARY
    + """

PRINCIPIOS DE FUNCIONAMIENTO

- Nunca inventar datos.
- Nunca asumir informacion que no este en el documento.
- Si falta informacion clave para clasificar con seguridad, usar vec_route = "manual_review".
- Si el PDF parece solo un enlace/codigo de validacion o no aporta campos criticos completos para VEC, usar vec_route = "manual_review" y anadir una alerta explicita.
- No permitir clasificaciones fuera del catalogo oficial.
- No permitir subir documentos invalidos.
- Solo usar campos documentados por el manual operativo.
- Prohibido completar informacion no solicitada por el documento.

REGLAS SOBRE FECHAS

- Nunca modificar fechas documentales.
- No unir periodos separados.
- No rellenar huecos entre contratos.
- Cada periodo de experiencia debe subirse como merito independiente.
- No permitir solapamientos.
- Si hay solapamiento publico-privado, priorizar publico.
- En formacion academica oficial, fecha inicio y fecha fin deben ser la fecha de expedicion o pago de tasas.

REGLAS SOBRE DOCUMENTACION

- No subir certificados incompletos.
- Si no aparece categoria profesional exacta en experiencia, advertir posible exclusion.
- No aceptar solicitudes pendientes como merito valido.
- En licencia radiactiva, solo es valido documento confirmado por CSN.
- Si no aparece nota media en formacion academica oficial, exigir certificado academico.
- Exigir vida laboral en otros centros sanitarios, otros centros no sanitarios, experiencia extracomunitaria y otras administraciones publicas.

REGLAS SOBRE ACREDITACIONES

- En formacion continuada con creditos CFC, marcar acreditado por Comision de Formacion Continuada del SNS.
- Si no tiene creditos, marcar "Sin animo de lucro reconocida por la administracion" cuando proceda.
- No inventar creditos.
- No convertir horas en creditos.

REGLAS SOBRE PUBLICACIONES

- No inventar indexacion.
- Si indexada, seleccionar base de datos exacta.
- Si hay factor de impacto, indicar cuartil obligatorio.
- ISSN obligatorio en revistas.
- ISBN o deposito legal obligatorio en libros.
- No inventar posicion de autor.

FORMACION CONTINUADA RECIBIDA

- Subapartados validos: formacion_continuada, diplomas_titulos_propios_universitarios, estancias_formativas.
- Si es formacion universitaria NO oficial (curso, experto, diploma, ECTS sin grado o master oficial), clasificar obligatoriamente en diplomas_titulos_propios_universitarios.
- Si incluye ECTS y no es grado o master oficial, clasificar en diplomas_titulos_propios_universitarios.

EXPERIENCIA

- SAS -> experiencia_sas.
- SNS no SAS -> usar la experiencia SNS del catalogo y, si no se puede reflejar en el JSON actual, dejar alerts claras y usar manual_review cuando sea necesario.
- Concertados -> otros centros sanitarios.
- Privados sin concierto -> no puntuan.

DOCUMENTOS PROVISIONALES Y VALIDACION

- Si aparece "provisional", anadir la alerta: "Documento provisional detectado. Es valido subirlo si no se dispone del definitivo."
- Si falta firma o sello, anadir: "FALTA FIRMA O SELLO OFICIAL. DOCUMENTO NO VALIDO."
- Si falta anverso o reverso, anadir: "FALTA ANVERSO O REVERSO. DOCUMENTACION INCOMPLETA."
- Si es CFC y no aparece logotipo CFC, anadir: "NO APARECE LOGOTIPO OFICIAL CFC. DOCUMENTO NO VALIDO COMO CFC."
- Los centros privados no puntuan sin concierto.

RESTRICCION ABSOLUTA

- Devuelve JSON unicamente.
- No uses markdown.
- No expliques nada fuera del JSON.
- No inventes centro, codigo de centro, categoria profesional, acreditacion, fechas, creditos, indexacion o posicion de autor.
- Si falta informacion critica, refleja la parada operativa con vec_route = "manual_review" y alerts explicitos.

SALIDA OBLIGATORIA PARA AUTOMATIZACION

- Mantener compatibilidad con la automatizacion existente.
- document_description y description deben ser tecnicos, breves y seguros para VEC. Intenta mantenerlos en 100 caracteres o menos.
- center_code_lookup_required debe ser true cuando el codigo oficial del centro deba consultarse en el catalogo.
- required_documents debe incluir documentacion adicional obligatoria cuando aplique.
- alerts debe incluir causas de exclusion, invalidez, manual review y riesgos de no puntuacion.
- En Step 4 de VEC la mayoria de los campos son desplegables. Devuelve valores listos para seleccionar en dropdown, no frases libres.
- Reconoce que la interfaz puede aparecer en espanol o en ingles. Aunque cambie el idioma visual, debes devolver el valor oficial del catalogo compatible con la automatizacion.
- Si un campo de Step 4 es dropdown y el documento no soporta una opcion exacta del catalogo, dejalo en blanco o usa manual_review; no inventes textos.
- Mapea de forma explicita los campos reales de Step 4:
  - "Tipo Formacion*" -> JSON key "training_type"
  - "Metodologia de Imparticion*" -> JSON key "delivery_method"
  - "Acreditada Recibida*" -> JSON key "accredited_choice" y solo admite "si" o "no"
  - "Codigo Curso" -> JSON key "course_code"
- El campo JSON "accredited_received" es la entidad acreditadora, no el selector si/no de Step 4.
- Si el PDF soporta esos cuatro campos de Step 4, no los dejes vacios.

Para los campos VEC existentes, usa estas opciones exactas cuando el PDF las soporte:
- scope: "Autonomico", "Nacional", "Comunitario", "Extracomunitario"
- training_type generico: "Congreso", "Curso", "Diploma de especializacion", "Jornada", "Master profesional", "Seminario", "Sesion Clinica", "Taller", "Otros"
- delivery_method: "Cursos online masivos y abiertos (OM)", "A distancia modalidad e-learning (V)", "Formacion Presencial (P)", "Semipresencial (S)", "A distancia (D)"
- credits_or_hours: "Creditos" o "Horas"
- accredited_choice: "si" o "no"
- accredited_received cuando proceda: "Acreditado por la Comision de Formacion Continuada Autonomica", "Acreditado por la Comision de Formacion Continuada del SNS", "Acreditado por el Consejo Internacional de Enfermeria", "Acreditado por European Accreditation Council For CME (EACCME)", "Acreditado por  American Medical Association (AMA)", "Acreditado por Royal College of Physicians and Surgeons of Canada"

Para diplomas y titulos propios universitarios, training_type debe usar exactamente uno de estos valores si el PDF lo soporta:
- "Master Titulo Propio"
- "Master Universitario No Grado Academico"
- "Titulo Propio Experto Universitario"
- "Titulo Propio Especialista Universitario"
- "Titulo Propio Diploma de Especializacion"
- "Cursos, Diplomas o Certificaciones de Extension Universitaria"
- "Titulo Propio Universitario distinto a los Anteriores"

Devuelve exactamente este JSON:
{
  "vec_route": "",
  "merit_name": "",
  "merit_type": "",
  "professional_category": "",
  "organizing_entity": "",
  "institution_name": "",
  "center_name": "",
  "center_search_text": "",
  "center_result_text": "",
  "center_code": "",
  "center_code_lookup_required": null,
  "start_date": "",
  "end_date": "",
  "scope": "",
  "training_type": "",
  "delivery_method": "",
  "accredited_choice": "",
  "accredited_received": "",
  "accreditation_cfc": null,
  "accreditation_type": "",
  "course_code": "",
  "credits_or_hours": "",
  "hours": null,
  "credits": null,
  "document_description": "",
  "description": "",
  "document_justification_only": false,
  "signature_or_stamp_present": null,
  "front_and_back_complete": null,
  "cfc_logo_present": null,
  "is_provisional_document": null,
  "is_private_center": null,
  "required_documents": [],
  "alerts": [],
  "year": null,
  "confidence": 0.0,
  "reasoning_summary": "",
  "field_evidence": {
    "merit_name": "",
    "merit_type": "",
    "professional_category": "",
    "organizing_entity": "",
    "institution_name": "",
    "center_name": "",
    "center_code": "",
    "start_date": "",
    "end_date": "",
    "scope": "",
    "training_type": "",
    "delivery_method": "",
    "credits_or_hours": "",
    "hours": "",
    "credits": "",
    "accredited_choice": "",
    "accredited_received": "",
    "accreditation_type": "",
    "course_code": "",
    "signature_or_stamp_present": "",
    "front_and_back_complete": "",
    "cfc_logo_present": "",
    "is_provisional_document": "",
    "is_private_center": ""
  }
}
"""
)

ROUTE_VALUES = {
    "formacion_continuada",
    "diplomas_titulos_propios_universitarios",
    "estancias_formativas",
    "experiencia_sas",
    "formacion_universitaria_grado",
    "formacion_academica",
    "otros_meritos",
    "manual_review",
    "unknown",
}

SUPPORTED_ROUTE_MARKERS = {
    "formacion_continuada": (
        "comision de formacion continuada",
        "actividad acreditada",
        "acreditada por",
        "creditos cfc",
        "formacion continuada",
    ),
    "diplomas_titulos_propios_universitarios": (
        "titulo propio",
        "titulacion propia",
        "experto universitario",
        "especialista universitario",
        "extension universitaria",
        "master de formacion permanente",
        "master titulo propio",
        "curso universitario",
        "certificado universitario",
        "diploma universitario",
    ),
    "estancias_formativas": (
        "estancia formativa",
        "training placement",
        "clinical placement",
    ),
}

UNSUPPORTED_ACADEMIC_ROUTE_MARKERS = (
    "master oficial",
    "master universitario oficial",
    "grado en",
    "grado universitario",
    "doctorado",
    "doctor en",
    "licenciado en",
    "diplomado en",
    "titulo oficial",
    "titulacion oficial",
    "laurea",
    "universita degli studi",
    "universidad oficial",
)

GENERIC_TRAINING_TYPE_VALUES = (
    "Congreso",
    "Curso",
    "Diploma de especializaciÃ³n",
    "Jornada",
    "Master profesional",
    "Seminario",
    "SesiÃ³n ClÃ­nica",
    "Taller",
    "Otros",
)

UNIVERSITY_TRAINING_TYPE_VALUES = (
    "Master TÃ­tulo Propio",
    "Master Universitario No Grado AcadÃ©mico",
    "TÃ­tulo Propio Experto Universitario",
    "TÃ­tulo Propio Especialista Universitario",
    "TÃ­tulo Propio Diploma de EspecializaciÃ³n",
    "Cursos, Diplomas o Certificaciones de ExtensiÃ³n Universitaria",
    "TÃ­tulo Propio Universitario distinto a los Anteriores",
)

DELIVERY_METHOD_VALUES = (
    "Cursos online masivos y abiertos (OM)",
    "A distancia modalidad e-learning (V)",
    "FormaciÃ³n Presencial (P)",
    "Semipresencial (S)",
    "A distancia (D)",
)

ACCREDITING_BODY_VALUES = (
    "Acreditado por la ComisiÃ³n de FormaciÃ³n Continuada AutonÃ³mica",
    "Acreditado por la ComisiÃ³n de FormaciÃ³n Continuada del SNS",
    "Acreditado por el Consejo Internacional de EnfermerÃ­a",
    "Acreditado por European Accreditation Council For CME (EACCME)",
    "Acreditado por  American Medical Association (AMA)",
    "Acreditado por Royal College of Physicians and Surgeons of Canada",
)

CREDITS_OR_HOURS_VALUES = ("CrÃ©ditos", "Horas")

FIXED_REQUIRED_DOCUMENTS = (
    "Documento completo",
    "Firma o sello",
    "Anverso y reverso",
)

TRAINING_ROUTE_VALUES = {
    "formacion_continuada",
    "diplomas_titulos_propios_universitarios",
    "estancias_formativas",
}

LINK_ONLY_MARKERS = (
    "para comprobar la autenticidad",
    "comprobar la autenticidad",
    "codigo de validacion",
    "codigo csv",
    "validar certificado",
    "validar_certificado",
    "verificar autenticidad",
    "acceder a este enlace",
    "version imprimible con informacion de firma",
    "valide",
)

CLASSIFICATION_COMPLETENESS_FIELD_LABELS = {
    "vec_route": "vec_route",
    "merit_name": "merit_name",
    "institution_name": "institution_name",
    "dates": "start_date/end_date",
    "training_type": "training_type",
    "delivery_method": "delivery_method",
    "duration": "credits_or_hours",
    "accredited_choice": "accredited_choice",
    "course_code": "course_code",
    "document_description": "document_description",
}

ROUTE_MERIT_TYPE_LABELS = {
    "formacion_continuada": "FormaciÃ³n continuada",
    "diplomas_titulos_propios_universitarios": "Diploma o tÃ­tulo propio universitario",
    "estancias_formativas": "Estancia formativa",
    "experiencia_sas": "Experiencia profesional SAS",
    "formacion_universitaria_grado": "FormaciÃ³n universitaria de grado",
    "formacion_academica": "FormaciÃ³n acadÃ©mica",
    "otros_meritos": "Otros mÃ©ritos",
    "manual_review": "RevisiÃ³n manual",
    "unknown": "",
}

SNS_ACCREDITATION_TYPE = "ComisiÃ³n de FormaciÃ³n Continuada del SNS"
UNIVERSITY_ACCREDITATION_TYPE = "Universitaria"
ADMIN_NONPROFIT_ACCREDITATION_TYPE = "Sin Ã¡nimo de lucro reconocida por la administraciÃ³n"

PROFESSIONAL_CATEGORY_PATTERNS = (
    ("tcae", "TCAE"),
    ("auxiliar de enfermer", "TCAE"),
    ("enfermer", "EnfermerÃ­a"),
    ("matron", "Matrona"),
    ("fisioterap", "Fisioterapia"),
    ("terapia ocupacional", "Terapia ocupacional"),
    ("logopeda", "Logopedia"),
    ("psicolog", "PsicologÃ­a"),
    ("trabajo social", "Trabajo social"),
    ("medic", "Medicina"),
    ("farmaci", "Farmacia"),
    ("celador", "Celador"),
    ("auxiliar administrativo", "Auxiliar administrativo"),
    ("administrativ", "Administrativo"),
    ("imagen para el diagnostico", "TSID y MN"),
    ("radiodiagnostico", "TSID y MN"),
    ("laboratorio", "Laboratorio"),
    ("tecnico especialista", "TÃ©cnico especialista"),
)

BOOLEAN_RESULT_FIELDS = {
    "document_justification_only",
    "center_code_lookup_required",
    "accreditation_cfc",
    "signature_or_stamp_present",
    "front_and_back_complete",
    "cfc_logo_present",
    "is_provisional_document",
    "is_private_center",
}

LIST_RESULT_FIELDS = {
    "required_documents",
    "alerts",
}

STRICT_AI_STRING_FIELDS = {
    "scope",
    "training_type",
    "delivery_method",
    "credits_or_hours",
    "accredited_choice",
    "accredited_received",
    "course_code",
    "center_result_text",
    "document_description",
}

STRICT_AI_NUMERIC_FIELDS = {
    "hours",
    "credits",
}

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

ANDALUSIA_KEYWORDS = {
    "andalucia",
    "sevilla",
    "malaga",
    "granada",
    "cadiz",
    "cordoba",
    "jaen",
    "huelva",
    "almeria",
}

EU_COUNTRY_KEYWORDS = {
    "union europea",
    "ue",
    "alemania",
    "germany",
    "francia",
    "france",
    "italia",
    "italy",
    "portugal",
    "belgica",
    "belgium",
    "paises bajos",
    "netherlands",
    "holanda",
    "irlanda",
    "ireland",
    "austria",
    "grecia",
    "greece",
    "polonia",
    "poland",
    "rumania",
    "romania",
    "suecia",
    "sweden",
    "finlandia",
    "finland",
    "dinamarca",
    "denmark",
    "croacia",
    "croatia",
}

EXTRA_EU_COUNTRY_KEYWORDS = {
    "reino unido",
    "united kingdom",
    "uk",
    "inglaterra",
    "switzerland",
    "suiza",
    "estados unidos",
    "united states",
    "usa",
    "canada",
    "mexico",
    "colombia",
    "argentina",
    "chile",
    "peru",
    "ecuador",
    "venezuela",
    "brasil",
    "brazil",
    "uruguay",
    "paraguay",
    "bolivia",
    "china",
    "japon",
    "japan",
    "india",
    "australia",
    "marruecos",
    "morocco",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract text from one PDF in downloaded_pdfs and classify it for VEC."
    )
    parser.add_argument(
        "--pdf",
        default=None,
        help="Optional path to a specific PDF file. If omitted, the script picks a random unprocessed PDF from --pdf-dir.",
    )
    parser.add_argument("--pdf-dir", default=PDF_DIRECTORY)
    parser.add_argument("--output", default=OUTPUT_JSON_FILE)
    parser.add_argument("--text-output", default=OUTPUT_TEXT_FILE)
    parser.add_argument(
        "--classification-backend",
        choices=("openai", "heuristic"),
        default="openai",
        help="AI classification backend.",
    )
    return parser.parse_args()


def repair_mojibake_text(value):
    text = str(value or "")
    mojibake_markers = ("Ã", "Â", "â", "Æ", "Å")

    for _ in range(3):
        if not any(marker in text for marker in mojibake_markers):
            break

        try:
            repaired = text.encode("latin-1").decode("utf-8")
        except Exception:
            break

        if repaired == text:
            break
        text = repaired

    return text


def normalize_text(value):
    normalized = unicodedata.normalize("NFKD", repair_mojibake_text(value))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_text).strip().casefold()


def normalize_pdf_path_key(path_value):
    if not path_value:
        return ""

    try:
        resolved = Path(path_value).resolve(strict=False)
    except Exception:
        resolved = Path(path_value)

    return os.path.normcase(os.path.normpath(str(resolved)))


def load_processed_pdf_registry(processed_file=PROCESSED_PDFS_FILE):
    registry_path = Path(processed_file)
    if not registry_path.exists():
        return []

    try:
        data = json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("processed_pdfs"), list):
        return data["processed_pdfs"]
    return []


def save_processed_pdf_registry(processed_entries, processed_file=PROCESSED_PDFS_FILE):
    registry_path = Path(processed_file)
    registry_path.write_text(
        json.dumps({"processed_pdfs": processed_entries}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def record_processed_pdf(source_pdf_path, archived_pdf_path="", status="processed", reason=""):
    source_path = Path(source_pdf_path) if source_pdf_path else None
    source_key = normalize_pdf_path_key(source_path)
    source_name = source_path.name if source_path else ""

    processed_entries = load_processed_pdf_registry()
    timestamp = dt.datetime.now().isoformat(timespec="seconds")

    existing_record = None
    for record in processed_entries:
        record_source = normalize_pdf_path_key(record.get("source_pdf_path"))
        record_archived = normalize_pdf_path_key(record.get("archived_pdf_path"))
        if source_key and source_key in (record_source, record_archived):
            existing_record = record
            break
        if source_name and record.get("source_pdf_filename") == source_name:
            existing_record = record
            break

    payload = {
        "processed_at": timestamp,
        "source_pdf_path": str(source_path) if source_path else "",
        "source_pdf_filename": source_name,
        "archived_pdf_path": str(archived_pdf_path or ""),
        "status": str(status or "").strip(),
        "reason": str(reason or "").strip(),
    }

    if existing_record is None:
        processed_entries.append(payload)
    else:
        existing_record.update(payload)

    save_processed_pdf_registry(processed_entries)


def load_skipped_pdf_registry(skipped_file=SKIPPED_PDFS_FILE):
    registry_path = Path(skipped_file)
    if not registry_path.exists():
        return []

    try:
        data = json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("skipped_pdfs"), list):
        return data["skipped_pdfs"]
    return []


def save_skipped_pdf_registry(skipped_entries, skipped_file=SKIPPED_PDFS_FILE):
    registry_path = Path(skipped_file)
    registry_path.write_text(
        json.dumps({"skipped_pdfs": skipped_entries}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def record_skipped_pdf(source_pdf_path, archived_pdf_path="", status="skipped", reason=""):
    source_path = Path(source_pdf_path) if source_pdf_path else None
    source_key = normalize_pdf_path_key(source_path)
    source_name = source_path.name if source_path else ""

    skipped_entries = load_skipped_pdf_registry()
    timestamp = dt.datetime.now().isoformat(timespec="seconds")

    existing_record = None
    for record in skipped_entries:
        record_source = normalize_pdf_path_key(record.get("source_pdf_path"))
        record_archived = normalize_pdf_path_key(record.get("archived_pdf_path"))
        if source_key and source_key in (record_source, record_archived):
            existing_record = record
            break
        if source_name and record.get("source_pdf_filename") == source_name:
            existing_record = record
            break

    payload = {
        "skipped_at": timestamp,
        "source_pdf_path": str(source_path) if source_path else "",
        "source_pdf_filename": source_name,
        "archived_pdf_path": str(archived_pdf_path or ""),
        "status": str(status or "").strip(),
        "reason": str(reason or "").strip(),
    }

    if existing_record is None:
        skipped_entries.append(payload)
    else:
        existing_record.update(payload)

    save_skipped_pdf_registry(skipped_entries)


def processed_pdf_keys(processed_entries):
    processed_paths = set()
    processed_names = set()

    for entry in processed_entries:
        for key in ("source_pdf_path", "archived_pdf_path"):
            normalized_path = normalize_pdf_path_key(entry.get(key))
            if normalized_path:
                processed_paths.add(normalized_path)

        filename = str(entry.get("source_pdf_filename") or "").strip()
        if filename:
            processed_names.add(normalize_text(filename))

    return processed_paths, processed_names


def is_processed_pdf(pdf_path, processed_paths, processed_names):
    path_key = normalize_pdf_path_key(pdf_path)
    name_key = normalize_text(Path(pdf_path).name)
    return path_key in processed_paths or name_key in processed_names


def next_archive_path(source_path, archive_dir):
    archive_dir.mkdir(parents=True, exist_ok=True)
    destination = archive_dir / source_path.name
    if not destination.exists():
        return destination

    stem = source_path.stem
    suffix = source_path.suffix
    counter = 2
    while True:
        candidate = archive_dir / f"{stem}-{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def skipped_archive_dir_for_status(status):
    folder_name = str(status or "skipped").strip().casefold()
    if folder_name.startswith("skipped_"):
        folder_name = folder_name[len("skipped_"):]
    folder_name = re.sub(r"[^a-z0-9._-]+", "_", folder_name).strip("._-") or "skipped"
    return SKIPPED_PDF_ARCHIVE_DIR / folder_name


def archive_skipped_pdf(pdf_path, status=""):
    source_path = Path(pdf_path)
    if not source_path.exists():
        return ""

    destination = next_archive_path(source_path, skipped_archive_dir_for_status(status))
    shutil.move(str(source_path), str(destination))
    return str(destination)


def skip_pdf_candidate(pdf_path, reason, status):
    archived_pdf_path = ""

    try:
        archived_pdf_path = archive_skipped_pdf(pdf_path, status=status)
    except Exception as exc:
        print(f"Warning: Could not archive skipped PDF {pdf_path}: {exc}")

    record_processed_pdf(
        pdf_path,
        archived_pdf_path=archived_pdf_path,
        status=status,
        reason=reason,
    )
    record_skipped_pdf(
        pdf_path,
        archived_pdf_path=archived_pdf_path,
        status=status,
        reason=reason,
    )

    print(f"Skipping PDF: {reason}")
    if archived_pdf_path:
        print("Archived skipped PDF:", archived_pdf_path)

    return archived_pdf_path


def is_valid_pdf_file(pdf_path):
    path = Path(pdf_path)
    if not path.exists() or not path.is_file() or path.stat().st_size == 0:
        return False

    with path.open("rb") as file_handle:
        return file_handle.read(5) == b"%PDF-"


def score_pdf_candidate(pdf_path):
    name = normalize_text(pdf_path.name)
    score = 0

    for keyword in ("certificado", "diploma", "curso", "master", "experto", "titulo"):
        if keyword in name:
            score += 3

    for keyword in ("servicios prestados", "serv merida", "vida laboral"):
        if keyword in name:
            score -= 10

    return score


def resolve_pdf_candidates(explicit_pdf_path, pdf_directory):
    processed_entries = load_processed_pdf_registry()
    processed_paths, processed_names = processed_pdf_keys(processed_entries)
    candidates = []
    seen = set()
    explicit_path_problem = ""

    def add_candidate(path, allow_processed=False):
        normalized_path = normalize_pdf_path_key(path)
        if not normalized_path or normalized_path in seen:
            return

        seen.add(normalized_path)
        if not allow_processed and is_processed_pdf(path, processed_paths, processed_names):
            return

        candidates.append(Path(path))

    if explicit_pdf_path:
        pdf_path = Path(explicit_pdf_path)
        if pdf_path.exists():
            if not is_valid_pdf_file(pdf_path):
                raise ValueError(f"File is not a valid PDF: {pdf_path}")
            add_candidate(pdf_path, allow_processed=True)
        else:
            explicit_path_problem = f"Requested PDF was not found and will be skipped: {pdf_path}"
            print(explicit_path_problem)

    pdf_dir = Path(pdf_directory)
    if pdf_dir.exists():
        pdf_files = [
            path
            for path in pdf_dir.rglob("*.pdf")
            if is_valid_pdf_file(path) and not is_processed_pdf(path, processed_paths, processed_names)
        ]
        if len(pdf_files) > 200:
            pdf_files = random.sample(pdf_files, 200)
        else:
            random.shuffle(pdf_files)
        for path in pdf_files:
            add_candidate(path)
    elif not candidates:
        raise FileNotFoundError(f"PDF directory not found: {pdf_dir}")

    if not candidates:
        if explicit_path_problem:
            raise FileNotFoundError(
                f"{explicit_path_problem} No unprocessed valid PDF files were available in: {pdf_dir}. "
                f"Processed uploads are tracked in {PROCESSED_PDFS_FILE}."
            )
        raise FileNotFoundError(
            f"No unprocessed valid PDF files found in: {pdf_dir}. "
            f"Processed uploads are tracked in {PROCESSED_PDFS_FILE}."
        )

    return candidates


load_dotenv()
client = None


def get_openai_client():
    global client

    if client is not None:
        return client

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in .env file")

    client = OpenAI(api_key=api_key)
    return client


def resolve_existing_path(path_value):
    file_path = Path(path_value).expanduser()
    if not file_path.is_absolute():
        file_path = (Path.cwd() / file_path).resolve()
    else:
        file_path = file_path.resolve()

    if not file_path.exists():
        raise FileNotFoundError(f"Required file not found: {file_path}")

    return file_path


def official_vec_reference_specs():
    return (
        (
            "manual_operativo_vec_sas",
            OFFICIAL_VEC_MANUAL_FILENAME,
            (
                os.getenv("OFFICIAL_VEC_MANUAL_PATH"),
                Path(OFFICIAL_VEC_MANUAL_FILENAME),
                Path("official_refs") / OFFICIAL_VEC_MANUAL_FILENAME,
                Path(PDF_DIRECTORY) / OFFICIAL_VEC_MANUAL_FILENAME,
                Path(r"c:\Users\USER\Downloads") / OFFICIAL_VEC_MANUAL_FILENAME,
            ),
        ),
        (
            "codigos_centros_vec_sas",
            OFFICIAL_VEC_CENTER_CODES_FILENAME,
            (
                os.getenv("OFFICIAL_VEC_CENTER_CODES_PATH"),
                Path(OFFICIAL_VEC_CENTER_CODES_FILENAME),
                Path("official_refs") / OFFICIAL_VEC_CENTER_CODES_FILENAME,
                Path(PDF_DIRECTORY) / OFFICIAL_VEC_CENTER_CODES_FILENAME,
                Path(r"c:\Users\USER\Downloads") / OFFICIAL_VEC_CENTER_CODES_FILENAME,
            ),
        ),
    )


def resolve_official_vec_reference_pdfs():
    resolved_references = []

    for reference_name, filename, candidates in official_vec_reference_specs():
        last_error = None
        for candidate in candidates:
            if not candidate:
                continue
            try:
                resolved_references.append((reference_name, resolve_existing_path(candidate)))
                break
            except FileNotFoundError as exc:
                last_error = exc
        else:
            if last_error is not None:
                raise last_error
            raise FileNotFoundError(f"Required reference PDF not found: {filename}")

    return tuple(resolved_references)


def upload_pdf_file_to_openai(path_value):
    file_path = resolve_existing_path(path_value)
    file_stat = file_path.stat()
    cache_key = (str(file_path), file_stat.st_size, file_stat.st_mtime_ns)
    cached_file_id = OPENAI_UPLOADED_FILE_IDS.get(cache_key)
    if cached_file_id:
        return cached_file_id

    with file_path.open("rb") as file_handle:
        uploaded = get_openai_client().files.create(file=file_handle, purpose="user_data")

    OPENAI_UPLOADED_FILE_IDS[cache_key] = uploaded.id
    return uploaded.id


def build_openai_file_content_items(pdf_path):
    main_pdf_path = resolve_existing_path(pdf_path)
    main_pdf_file_id = upload_pdf_file_to_openai(main_pdf_path)

    content_items = [
        {
            "type": "input_text",
            "text": f"PDF principal del merito a clasificar: {main_pdf_path.name}",
        },
        {
            "type": "input_file",
            "file_id": main_pdf_file_id,
            "filename": main_pdf_path.name,
        },
    ]

    for reference_name, resolved_reference_path in resolve_official_vec_reference_pdfs():
        reference_file_id = upload_pdf_file_to_openai(resolved_reference_path)
        content_items.extend(
            [
                {
                    "type": "input_text",
                    "text": (
                        "PDF oficial de referencia obligatorio para la clasificacion VEC: "
                        f"{reference_name} -> {resolved_reference_path.name}"
                    ),
                },
                {
                    "type": "input_file",
                    "file_id": reference_file_id,
                    "filename": resolved_reference_path.name,
                },
            ]
        )

    return content_items


def build_openai_attached_paths(pdf_path):
    attached_paths = [resolve_existing_path(pdf_path)]
    for _, reference_path in resolve_official_vec_reference_pdfs():
        attached_paths.append(reference_path)
    return attached_paths


def save_openai_request_preview(system_prompt, user_prompt, attached_paths):
    preview_payload = {
        "system_engine": str(system_prompt or ""),
        "user_prompt": str(user_prompt or ""),
        "attached_files": [str(resolve_existing_path(path_value)) for path_value in attached_paths],
    }
    try:
        OPENAI_REQUEST_PREVIEW_FILE.write_text(
            json.dumps(preview_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass


def extract_pdf_text(pdf_path):
    full_text = ""

    with fitz.open(pdf_path) as doc:
        for page in doc:
            full_text += page.get_text()

    return full_text.strip()


def extract_json_object_from_text(text):
    raw_text = str(text or "").strip()
    if not raw_text:
        raise ValueError("ChatGPT returned an empty response.")

    candidates = [raw_text]
    candidates.extend(re.findall(r"```json\s*(\{.*?\})\s*```", raw_text, flags=re.IGNORECASE | re.DOTALL))
    candidates.extend(re.findall(r"```\s*(\{.*?\})\s*```", raw_text, flags=re.DOTALL))
    decoder = json.JSONDecoder()

    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            for index, char in enumerate(candidate):
                if char != "{":
                    continue
                try:
                    payload, _ = decoder.raw_decode(candidate[index:])
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    return payload
        else:
            if isinstance(payload, dict):
                return payload

    raise ValueError("Could not parse a JSON object from the ChatGPT response.")


def has_extractable_text(document_text):
    return bool((document_text or "").strip())


def blank_result():
    return {
        "source_pdf_path": "",
        "source_pdf_filename": "",
        "vec_route": "unknown",
        "merit_name": "",
        "merit_type": "",
        "professional_category": "",
        "organizing_entity": "",
        "institution_name": "",
        "center_name": "",
        "center_search_text": "",
        "center_result_text": "",
        "center_code": "",
        "center_code_lookup_required": None,
        "start_date": "",
        "end_date": "",
        "scope": "",
        "training_type": "",
        "delivery_method": "",
        "accredited_choice": "",
        "accredited_received": "",
        "accreditation_cfc": None,
        "accreditation_type": "",
        "course_code": "",
        "credits_or_hours": "",
        "hours": 0,
        "credits": 0,
        "document_description": "",
        "description": "",
        "document_justification_only": False,
        "signature_or_stamp_present": None,
        "front_and_back_complete": None,
        "cfc_logo_present": None,
        "is_provisional_document": None,
        "is_private_center": None,
        "required_documents": list(FIXED_REQUIRED_DOCUMENTS),
        "alerts": [],
        "year": 0,
        "confidence": 0.0,
        "reasoning_summary": "",
        "field_evidence": {
            "merit_name": "",
            "merit_type": "",
            "professional_category": "",
            "organizing_entity": "",
            "institution_name": "",
            "center_name": "",
            "center_code": "",
            "start_date": "",
            "end_date": "",
            "scope": "",
            "training_type": "",
            "delivery_method": "",
            "credits_or_hours": "",
            "hours": "",
            "credits": "",
            "accredited_choice": "",
            "accredited_received": "",
            "accreditation_type": "",
            "course_code": "",
            "signature_or_stamp_present": "",
            "front_and_back_complete": "",
            "cfc_logo_present": "",
            "is_provisional_document": "",
            "is_private_center": "",
        },
        "vec_step_payload": {
            "step_1_dates": {
                "start_date": "",
                "end_date": "",
            },
            "step_2_center": {
                "institution_name": "",
                "organizing_entity": "",
                "center_name": "",
                "center_search_text": "",
                "center_result_text": "",
                "center_code": "",
                "center_code_lookup_required": None,
            },
            "step_3_merit_name": {
                "merit_name": "",
                "description": "",
                "document_description": "",
            },
            "step_4_specific_values": {
                "professional_category": "",
                "scope": "",
                "training_type": "",
                "delivery_method": "",
                "accredited_choice": "",
                "accredited_received": "",
                "accreditation_type": "",
                "course_code": "",
                "credits_or_hours": "",
                "hours": 0,
                "credits": 0,
            },
            "step_5_documents": {
                "document_description": "",
                "description": "",
                "required_documents": [],
                "alerts": [],
                "signature_or_stamp_present": None,
                "front_and_back_complete": None,
                "cfc_logo_present": None,
                "is_provisional_document": None,
                "is_private_center": None,
                "document_justification_only": False,
            },
        },
    }


def clean_text(value):
    repaired = repair_mojibake_text(value)
    cleaned = re.sub(r"\s+", " ", repaired.replace("\n", " ")).strip(" :;,.\"'“”‘’")
    return cleaned


def compact_evidence_text(value, max_length=240):
    text = clean_text(value)
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip(" ,.;:") + "..."


def snippet_from_span(text, start, end, radius=120, max_length=240):
    source = repair_mojibake_text(text)
    if not source:
        return ""
    left = max(0, start - radius)
    right = min(len(source), end + radius)
    return compact_evidence_text(source[left:right], max_length=max_length)


def snippet_from_patterns(document_text, patterns, radius=120, max_length=240):
    search_text = repair_mojibake_text(document_text)
    if not search_text:
        return ""

    for pattern in patterns:
        match = re.search(pattern, search_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return snippet_from_span(
                search_text,
                match.start(),
                match.end(),
                radius=radius,
                max_length=max_length,
            )

    return ""


def snippet_from_value(document_text, value, radius=120, max_length=240):
    search_text = repair_mojibake_text(document_text)
    target = clean_text(value)
    if not search_text or not target:
        return ""

    match = re.search(re.escape(target), search_text, flags=re.IGNORECASE)
    if not match:
        return ""

    return snippet_from_span(
        search_text,
        match.start(),
        match.end(),
        radius=radius,
        max_length=max_length,
    )


def bool_evidence(value, true_text="", false_text="", unknown_text=""):
    coerced = coerce_optional_bool(value)
    if coerced is True:
        return compact_evidence_text(true_text)
    if coerced is False:
        return compact_evidence_text(false_text)
    return compact_evidence_text(unknown_text)


def build_field_evidence(result, heuristic_result, document_text=""):
    blank_evidence = blank_result()["field_evidence"]
    existing = result.get("field_evidence")
    if not isinstance(existing, dict):
        existing = {}
    canonical_hours, canonical_credits, canonical_units = canonicalize_duration_selection(
        result.get("credits"),
        result.get("hours"),
    )

    date_range_snippet = snippet_from_patterns(
        document_text,
        [
            r"realizado del\s+.+?\s+al\s+.+?(?:,|\.)",
            r"celebrado del\s+.+?\s+al\s+.+?(?:,|\.)",
            r"fecha inicio estudios:\s*.+?fecha fin estudios:\s*.+?(?:,|\.)",
        ],
    )
    duration_snippet = snippet_from_patterns(
        document_text,
        [
            r"duraci[oó]n de\s+\d+(?:[.,]\d+)?\s*horas(?:\s+y\s+\d+(?:[.,]\d+)?\s*cr[eé]ditos?)?",
            r"\d+(?:[.,]\d+)?\s*horas\s+y\s+\d+(?:[.,]\d+)?\s*cr[eé]ditos",
            r"\d+(?:[.,]\d+)?\s*cr[eé]ditos(?:\s*ects)?",
        ],
    )
    accreditation_snippet = snippet_from_patterns(
        document_text,
        [
            r"actividad acreditada por\s+.+?(?:,|\.)",
            r"acreditada por\s+.+?(?:,|\.)",
            r"comisi[oó]n de formaci[oó]n continuada.+?(?:,|\.)",
        ],
    )
    organizing_entity_snippet = snippet_from_patterns(
        document_text,
        [
            r"organizad[oa]\s+por\s+.+?(?:,?\s+celebrad[oa]|,?\s+realizad[oa]|,|\.)",
            r"documento firmado por\s+.+?(?:,|\.)",
            r"firmad[oa]\s+digitalmente\s+por\s+.+?(?:,|\.)",
            r"ACMA\s+ASOCIACI[ÓO]N.+?(?:,|\.)",
            r"universidad\s+.+?(?:,|\.)",
        ],
    )
    signature_snippet = snippet_from_patterns(
        document_text,
        [
            r"Firmado por:\s*.+",
            r"firma(?:do)?\s+por\s*.+",
            r"Secretaria\s+[A-ZÁÉÍÓÚÑ].+",
        ],
    )
    category_snippet = snippet_from_patterns(
        document_text,
        [
            r"dirigido a\s+[^,.]+",
            r"categor[ií]a\s+profesional[:\s]+[^,.]+",
        ],
    )
    course_code_snippet = snippet_from_patterns(
        document_text,
        [
            r"n[ºo]\s+de\s+Expediente\s+[A-Za-z0-9/-]+",
            r"expediente\s+[A-Za-z0-9/-]+",
            r"c[oó]digo(?:\s+del\s+curso|\s+curso)\s*[:\-]?\s*[A-Za-z0-9/-]+",
        ],
    )
    scope_snippet = snippet_from_patterns(
        document_text,
        [
            r"[ÁA]mbito\s+Nacional",
            r"[ÁA]mbito\s+Auton[oó]mico",
            r"[ÁA]mbito\s+Comunitario",
            r"[ÁA]mbito\s+Extracomunitario",
            r"Asociaci[oó]n\s+de\s+[ÁA]mbito\s+Nacional",
        ],
    )
    training_snippet = snippet_from_patterns(
        document_text,
        [
            r"el\s+curso[:\s]+.+?(?:realizado|celebrado|dirigido a)",
            r"\bcurso\b",
            r"\bcongreso\b",
            r"\bjornada\b",
            r"\bseminario\b",
            r"\btaller\b",
            r"\bmaster\b",
            r"\bdiploma\b",
        ],
    )
    center_code_snippet = snippet_from_patterns(
        document_text,
        [
            r"c[oó]digo\s+de\s+centro[:\s]+[A-Za-z0-9/-]+",
            r"c[oó]digo\s+centro[:\s]+[A-Za-z0-9/-]+",
            r"center\s+code[:\s]+[A-Za-z0-9/-]+",
        ],
    )
    delivery_snippet = snippet_from_patterns(
        document_text,
        [
            r"(?:modalidad|metodolog[ií]a|impartici[oó]n)[:\s]+[^,.]+",
            r"\bsemipresencial\b",
            r"\bpresencial\b",
            r"\be-learning\b",
            r"\bonline\b",
            r"\ba distancia\b",
        ],
    )
    provisional_snippet = snippet_from_patterns(
        document_text,
        [
            r"\bprovisional\b",
            r"\bsupletori[ao]\b",
            r"pendiente de expedici[oó]n",
        ],
    )

    generated = {
        "merit_name": first_non_empty_text(
            result.get("merit_name"),
            existing.get("merit_name"),
            training_snippet,
        ),
        "merit_type": first_non_empty_text(
            result.get("merit_type"),
            existing.get("merit_type"),
            training_snippet,
        ),
        "professional_category": first_non_empty_text(
            result.get("professional_category"),
            existing.get("professional_category"),
            category_snippet,
        ),
        "organizing_entity": first_non_empty_text(
            result.get("organizing_entity"),
            existing.get("organizing_entity"),
            organizing_entity_snippet,
        ),
        "institution_name": first_non_empty_text(
            result.get("institution_name"),
            existing.get("institution_name"),
            organizing_entity_snippet,
        ),
        "center_name": first_non_empty_text(
            result.get("center_name"),
            existing.get("center_name"),
            organizing_entity_snippet,
        ),
        "center_code": first_non_empty_text(
            result.get("center_code"),
            existing.get("center_code"),
            center_code_snippet,
            "No aparece codigo de centro oficial en el PDF; requiere consulta en el catalogo oficial."
            if result.get("center_code_lookup_required")
            else result.get("center_code"),
        ),
        "start_date": first_non_empty_text(
            result.get("start_date"),
            existing.get("start_date"),
            date_range_snippet,
        ),
        "end_date": first_non_empty_text(
            result.get("end_date"),
            existing.get("end_date"),
            date_range_snippet,
        ),
        "scope": first_non_empty_text(
            result.get("scope"),
            existing.get("scope"),
            scope_snippet,
        ),
        "training_type": first_non_empty_text(
            result.get("training_type"),
            existing.get("training_type"),
            training_snippet,
        ),
        "delivery_method": first_non_empty_text(
            result.get("delivery_method"),
            existing.get("delivery_method"),
            delivery_snippet,
            (
                f"{result.get('delivery_method')}. Derivado por regla VEC/Manual Operativo al no aparecer "
                "una modalidad presencial o semipresencial explicita en el PDF."
            )
            if result.get("delivery_method")
            else "",
        ),
        "credits_or_hours": first_non_empty_text(
            result.get("credits_or_hours"),
            existing.get("credits_or_hours"),
            canonical_units,
        ),
        "hours": (
            first_non_empty_text(
                str(canonical_hours or ""),
                existing.get("hours"),
                duration_snippet,
            )
            if canonical_hours not in (None, "", 0)
            else ""
        ),
        "credits": first_non_empty_text(
            str(canonical_credits or ""),
            existing.get("credits"),
            duration_snippet,
        ),
        "accredited_choice": first_non_empty_text(
            result.get("accredited_choice"),
            existing.get("accredited_choice"),
            accreditation_snippet,
        ),
        "accredited_received": first_non_empty_text(
            result.get("accredited_received"),
            existing.get("accredited_received"),
            accreditation_snippet,
        ),
        "accreditation_type": first_non_empty_text(
            result.get("accreditation_type"),
            existing.get("accreditation_type"),
            accreditation_snippet,
        ),
        "course_code": first_non_empty_text(
            result.get("course_code"),
            existing.get("course_code"),
            course_code_snippet,
        ),
        "signature_or_stamp_present": first_non_empty_text(
            bool_evidence(
                result.get("signature_or_stamp_present"),
                true_text="Se detecta firma/sello en el PDF o en el texto extraido.",
                false_text="No se detecta firma ni sello oficial en el texto extraido.",
                unknown_text="Sin evidencia textual concluyente sobre firma o sello.",
            ),
            existing.get("signature_or_stamp_present"),
            signature_snippet,
        ),
        "front_and_back_complete": first_non_empty_text(
            bool_evidence(
                result.get("front_and_back_complete"),
                true_text="El texto extraido indica anverso y reverso o ambas caras.",
                false_text="El texto extraido indica que falta anverso o reverso.",
                unknown_text="Sin evidencia textual clara sobre anverso/reverso en el texto extraido.",
            ),
            existing.get("front_and_back_complete"),
        ),
        "cfc_logo_present": first_non_empty_text(
            bool_evidence(
                result.get("cfc_logo_present"),
                true_text="El texto extraido menciona logotipo o logo CFC.",
                false_text="El texto extraido indica ausencia de logotipo CFC.",
                unknown_text="Sin evidencia textual clara sobre el logotipo CFC en el texto extraido.",
            ),
            existing.get("cfc_logo_present"),
        ),
        "is_provisional_document": first_non_empty_text(
            bool_evidence(
                result.get("is_provisional_document"),
                true_text="El documento se describe como provisional o supletorio.",
                false_text="No aparece el termino provisional ni equivalente en el texto extraido.",
                unknown_text="Sin evidencia textual clara sobre caracter provisional.",
            ),
            existing.get("is_provisional_document"),
            provisional_snippet,
        ),
        "is_private_center": first_non_empty_text(
            bool_evidence(
                result.get("is_private_center"),
                true_text="El texto del centro sugiere entidad privada o mercantil.",
                false_text="El texto del centro sugiere entidad publica, asociativa o sin animo de lucro.",
                unknown_text="Sin evidencia textual concluyente sobre titularidad privada del centro.",
            ),
            existing.get("is_private_center"),
        ),
    }

    return {
        key: compact_evidence_text(generated.get(key) or blank_evidence.get(key))
        for key in blank_evidence
    }


def build_vec_step_payload(result):
    return {
        "step_1_dates": {
            "start_date": result.get("start_date") or "",
            "end_date": result.get("end_date") or "",
        },
        "step_2_center": {
            "institution_name": result.get("institution_name") or "",
            "organizing_entity": result.get("organizing_entity") or "",
            "center_name": result.get("center_name") or "",
            "center_search_text": result.get("center_search_text") or "",
            "center_result_text": result.get("center_result_text") or "",
            "center_code": result.get("center_code") or "",
            "center_code_lookup_required": result.get("center_code_lookup_required"),
        },
        "step_3_merit_name": {
            "merit_name": result.get("merit_name") or "",
            "description": result.get("description") or "",
            "document_description": result.get("document_description") or "",
        },
        "step_4_specific_values": {
            "professional_category": result.get("professional_category") or "",
            "scope": result.get("scope") or "",
            "training_type": result.get("training_type") or "",
            "delivery_method": result.get("delivery_method") or "",
            "accredited_choice": result.get("accredited_choice") or "",
            "accredited_received": result.get("accredited_received") or "",
            "accreditation_type": result.get("accreditation_type") or "",
            "course_code": result.get("course_code") or "",
            "credits_or_hours": result.get("credits_or_hours") or "",
            "hours": result.get("hours", 0),
            "credits": result.get("credits", 0),
        },
        "step_5_documents": {
            "document_description": result.get("document_description") or "",
            "description": result.get("description") or "",
            "required_documents": clean_string_list(result.get("required_documents")),
            "alerts": clean_string_list(result.get("alerts")),
            "signature_or_stamp_present": result.get("signature_or_stamp_present"),
            "front_and_back_complete": result.get("front_and_back_complete"),
            "cfc_logo_present": result.get("cfc_logo_present"),
            "is_provisional_document": result.get("is_provisional_document"),
            "is_private_center": result.get("is_private_center"),
            "document_justification_only": result.get("document_justification_only"),
        },
    }


def normalize_choice_from_options(value, options):
    cleaned = clean_text(value)
    normalized = normalize_text(cleaned)
    if not normalized:
        return ""

    for option in options:
        if normalize_text(option) == normalized:
            return repair_mojibake_text(option)

    for option in options:
        option_normalized = normalize_text(option)
        if normalized in option_normalized or option_normalized in normalized:
            return repair_mojibake_text(option)

    return ""


def normalize_training_type_value(value, route="unknown"):
    route = str(route or "").strip()
    options = (
        UNIVERSITY_TRAINING_TYPE_VALUES
        if route == "diplomas_titulos_propios_universitarios"
        else GENERIC_TRAINING_TYPE_VALUES
    )
    exact_match = normalize_choice_from_options(value, options)
    if exact_match:
        return exact_match

    normalized = normalize_text(value)
    if not normalized:
        return ""

    if route == "diplomas_titulos_propios_universitarios":
        diploma_mappings = (
            ("master de formacion permanente", "Master Universitario No Grado AcadÃ©mico"),
            ("master universitario no grado academico", "Master Universitario No Grado AcadÃ©mico"),
            ("master titulo propio", "Master TÃ­tulo Propio"),
            ("titulo propio experto universitario", "TÃ­tulo Propio Experto Universitario"),
            ("experto universitario", "TÃ­tulo Propio Experto Universitario"),
            ("titulo propio especialista universitario", "TÃ­tulo Propio Especialista Universitario"),
            ("especialista universitario", "TÃ­tulo Propio Especialista Universitario"),
            ("titulo propio diploma de especializacion", "TÃ­tulo Propio Diploma de EspecializaciÃ³n"),
            ("diploma de especializacion", "TÃ­tulo Propio Diploma de EspecializaciÃ³n"),
            (
                "cursos, diplomas o certificaciones de extension universitaria",
                "Cursos, Diplomas o Certificaciones de ExtensiÃ³n Universitaria",
            ),
            ("curso universitario", "Cursos, Diplomas o Certificaciones de ExtensiÃ³n Universitaria"),
            ("certificado universitario", "Cursos, Diplomas o Certificaciones de ExtensiÃ³n Universitaria"),
            ("diploma universitario", "Cursos, Diplomas o Certificaciones de ExtensiÃ³n Universitaria"),
            (
                "titulo propio universitario distinto a los anteriores",
                "TÃ­tulo Propio Universitario distinto a los Anteriores",
            ),
            ("titulo propio", "TÃ­tulo Propio Universitario distinto a los Anteriores"),
            ("master", "Master TÃ­tulo Propio"),
        )
        for token, option in diploma_mappings:
            if token in normalized:
                return option
        return ""

    generic_mappings = (
        ("sesion clinica", "SesiÃ³n ClÃ­nica"),
        ("clinical session", "SesiÃ³n ClÃ­nica"),
        ("congreso", "Congreso"),
        ("congress", "Congreso"),
        ("conference", "Congreso"),
        ("symposium", "Congreso"),
        ("simposio", "Congreso"),
        ("seminario", "Seminario"),
        ("seminar", "Seminario"),
        ("webinar", "Seminario"),
        ("taller", "Taller"),
        ("workshop", "Taller"),
        ("jornada", "Jornada"),
        ("master profesional", "Master profesional"),
        ("professional master", "Master profesional"),
        ("specialization diploma", "Diploma de especializaciÃ³n"),
        ("specialisation diploma", "Diploma de especializaciÃ³n"),
        ("diploma de especializacion", "Diploma de especializaciÃ³n"),
        ("master", "Master profesional"),
        ("curso", "Curso"),
        ("course", "Curso"),
        ("training", "Curso"),
        ("certificate", "Curso"),
        ("certificado", "Curso"),
        ("otros", "Otros"),
        ("other", "Otros"),
    )
    for token, option in generic_mappings:
        if token in normalized:
            return option

    return ""


def normalize_delivery_method_value(value):
    exact_match = normalize_choice_from_options(value, DELIVERY_METHOD_VALUES)
    if exact_match:
        return exact_match

    normalized = normalize_text(value)
    if not normalized:
        return ""

    if any(token in normalized for token in ("om)", "mooc", "online masivos", "abiertos")):
        return "Cursos online masivos y abiertos (OM)"
    if any(token in normalized for token in ("e-learning", "elearning", "virtual", "online", "on line")):
        return "A distancia modalidad e-learning (V)"
    if any(token in normalized for token in ("semipresencial", "mixta", "blended")):
        return "Semipresencial (S)"
    if "presencial" in normalized or "face to face" in normalized or "in person" in normalized:
        return "FormaciÃ³n Presencial (P)"
    if "a distancia" in normalized or "distance" in normalized or "no presencial" in normalized:
        return "A distancia (D)"

    return ""


def normalize_credits_or_hours_value(value):
    exact_match = normalize_choice_from_options(value, CREDITS_OR_HOURS_VALUES)
    if exact_match:
        return exact_match

    normalized = normalize_text(value)
    if not normalized:
        return ""

    if normalized.startswith("credit") or "ects" in normalized:
        return "CrÃ©ditos"
    if normalized.startswith("hora") or "hours" in normalized:
        return "Horas"

    return ""


def normalize_yes_no_value(value):
    normalized = normalize_text(value)
    if normalized in ("si", "yes", "true", "1"):
        return "si"
    if normalized in ("no", "false", "0"):
        return "no"
    return ""


def dedupe_preserve_order(values):
    ordered = []
    seen = set()

    for value in values:
        cleaned = clean_text(value)
        if not cleaned:
            continue

        normalized = normalize_text(cleaned)
        if normalized in seen:
            continue

        seen.add(normalized)
        ordered.append(cleaned)

    return ordered


def clean_string_list(values):
    if values in (None, ""):
        return []

    if isinstance(values, str):
        values = re.split(r"[\r\n]+", values)

    if not isinstance(values, (list, tuple, set)):
        return []

    return dedupe_preserve_order(values)


def coerce_optional_bool(value):
    if isinstance(value, bool):
        return value

    if value is None:
        return None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False

    normalized = normalize_text(value)
    if normalized in ("si", "yes", "true", "1"):
        return True
    if normalized in ("no", "false", "0"):
        return False

    return None


def first_non_empty_text(*values):
    for value in values:
        cleaned = clean_text(value)
        if cleaned:
            return cleaned
    return ""


def first_known_bool(*values):
    for value in values:
        coerced = coerce_optional_bool(value)
        if coerced is not None:
            return coerced
    return None


def is_university_context(route="", *texts):
    normalized_route = str(route or "").strip()
    if normalized_route in (
        "diplomas_titulos_propios_universitarios",
        "formacion_universitaria_grado",
        "formacion_academica",
    ):
        return True

    combined = normalize_text(" ".join(str(text or "") for text in texts))
    return any(token in combined for token in ("universidad", "universitario", "universitaria", "facultad"))


def derive_merit_type(route, training_type="", merit_name=""):
    explicit_training_type = clean_text(training_type)
    if explicit_training_type:
        return explicit_training_type

    normalized_name = normalize_text(merit_name)
    if "experiencia" in normalized_name:
        return "Experiencia"

    return ROUTE_MERIT_TYPE_LABELS.get(str(route or "").strip(), "")


def extract_professional_category(document_text, merit_name="", institution_name="", description=""):
    normalized = normalize_text(f"{merit_name} {institution_name} {description} {document_text}")
    if not normalized:
        return ""

    for token, category in PROFESSIONAL_CATEGORY_PATTERNS:
        if token in normalized:
            return category

    return ""


def extract_center_name(institution_name, center_search_text="", center_result_text=""):
    return first_non_empty_text(institution_name, center_search_text, center_result_text)


def extract_center_code(document_text):
    patterns = [
        r"c[oÃƒÂ³]digo de centro[:\s]*([A-Za-z0-9/-]+)",
        r"c[oÃƒÂ³]digo centro[:\s]*([A-Za-z0-9/-]+)",
        r"center code[:\s]*([A-Za-z0-9/-]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, document_text, flags=re.IGNORECASE)
        if match:
            return clean_text(match.group(1))

    return ""


def derive_accreditation_cfc(result, heuristic_result, document_text=""):
    explicit = first_known_bool(
        result.get("accreditation_cfc"),
        heuristic_result.get("accreditation_cfc"),
    )
    if explicit is not None:
        return explicit

    bodies = [
        normalize_text(result.get("accredited_received")),
        normalize_text(heuristic_result.get("accredited_received")),
        normalize_text(document_text),
    ]
    if any("comision de formacion continuada" in body for body in bodies):
        return True

    if any(body for body in bodies[:2]):
        return False

    accredited_choice = normalize_yes_no_value(result.get("accredited_choice")) or normalize_yes_no_value(
        heuristic_result.get("accredited_choice")
    )
    if accredited_choice == "no":
        return False

    return None


def derive_accreditation_type(result, heuristic_result, document_text=""):
    explicit = first_non_empty_text(
        result.get("accreditation_type"),
        heuristic_result.get("accreditation_type"),
    )
    if explicit:
        return explicit

    if derive_accreditation_cfc(result, heuristic_result, document_text=document_text) is True:
        return SNS_ACCREDITATION_TYPE

    route = str(result.get("vec_route") or heuristic_result.get("vec_route") or "").strip()
    if is_university_context(
        route,
        result.get("institution_name"),
        heuristic_result.get("institution_name"),
        result.get("organizing_entity"),
        heuristic_result.get("organizing_entity"),
        result.get("merit_name"),
        heuristic_result.get("merit_name"),
        document_text,
    ):
        return UNIVERSITY_ACCREDITATION_TYPE

    has_context = bool(
        first_non_empty_text(
            result.get("merit_name"),
            heuristic_result.get("merit_name"),
            result.get("institution_name"),
            heuristic_result.get("institution_name"),
            result.get("organizing_entity"),
            heuristic_result.get("organizing_entity"),
        )
    )
    return ADMIN_NONPROFIT_ACCREDITATION_TYPE if has_context else ""


def detect_signature_or_stamp_present(document_text):
    normalized = normalize_text(document_text)
    if not normalized:
        return None

    if any(token in normalized for token in ("sin firma", "sin sello", "unsigned")):
        return False

    positive_markers = (
        "firma",
        "firmado por",
        "firmado electronicamente",
        "signed by",
        "sello",
        "stamp",
    )
    if any(token in normalized for token in positive_markers):
        return True

    return None


def detect_front_and_back_complete(document_text):
    normalized = normalize_text(document_text)
    if not normalized:
        return None

    if any(token in normalized for token in ("falta reverso", "solo anverso", "solo reverso")):
        return False

    if any(token in normalized for token in ("anverso y reverso", "ambas caras", "cara anterior y posterior")):
        return True

    return None


def detect_cfc_logo_present(document_text):
    normalized = normalize_text(document_text)
    if not normalized:
        return None

    if any(token in normalized for token in ("sin logotipo cfc", "sin logo cfc")):
        return False

    if "logotipo" in normalized and "cfc" in normalized:
        return True

    return None


def detect_provisional_document(document_text):
    normalized = normalize_text(document_text)
    if not normalized:
        return None

    provisional_markers = (
        "provisional",
        "provisionally",
        "supletorio",
        "supletoria",
        "pendiente de expedicion",
        "pendiente de expedicion",
    )
    if any(token in normalized for token in provisional_markers):
        return True

    return False


def detect_private_center(document_text, institution_name=""):
    institution_text = clean_text(institution_name)
    normalized = normalize_text(f"{institution_text} {document_text}")
    if not normalized:
        return None

    public_markers = (
        "servicio andaluz de salud",
        "junta de andalucia",
        "ministerio",
        "colegio oficial",
        "asociacion",
        "federacion",
        "universidad publica",
    )
    if any(token in normalized for token in public_markers):
        return False

    if "universidad privada" in normalized or "centro privado" in normalized:
        return True

    lowered_institution = institution_text.lower()
    legal_private_markers = (" s.l.", " s.l", " slu", " s.a.", " s.a", " slp")
    if any(marker in lowered_institution for marker in legal_private_markers):
        return True

    textual_private_markers = ("academia", "academy", "empresa privada", "instituto privado")
    if any(token in normalized for token in textual_private_markers):
        return True

    return None


def derive_required_documents(result, heuristic_result):
    return dedupe_preserve_order(
        list(FIXED_REQUIRED_DOCUMENTS)
        + clean_string_list(result.get("required_documents"))
        + clean_string_list(heuristic_result.get("required_documents"))
    )


def build_rule_alerts(result, heuristic_result):
    alerts = clean_string_list(result.get("alerts")) + clean_string_list(heuristic_result.get("alerts"))

    if result.get("signature_or_stamp_present") is False:
        alerts.append("FALTA FIRMA O SELLO OFICIAL. DOCUMENTO NO VALIDO.")
    if result.get("front_and_back_complete") is False:
        alerts.append("FALTA ANVERSO O REVERSO. DOCUMENTACION INCOMPLETA.")
    if result.get("accreditation_cfc") is True and result.get("cfc_logo_present") is False:
        alerts.append("NO APARECE LOGOTIPO OFICIAL CFC. DOCUMENTO NO VALIDO COMO CFC.")
    if result.get("is_provisional_document") is True:
        alerts.append("Documento provisional detectado. Es valido subirlo si no se dispone del definitivo.")
    if result.get("is_private_center") is True:
        alerts.append("NO PUNTUA")

    return dedupe_preserve_order(alerts)


def enrich_result_with_rule_fields(result, heuristic_result, document_text=""):
    route = str(result.get("vec_route") or heuristic_result.get("vec_route") or "").strip()
    extracted_institution_name = extract_institution_name(document_text)
    result["merit_type"] = first_non_empty_text(
        result.get("merit_type"),
        heuristic_result.get("merit_type"),
        derive_merit_type(
            route,
            training_type=first_non_empty_text(result.get("training_type"), heuristic_result.get("training_type")),
            merit_name=first_non_empty_text(result.get("merit_name"), heuristic_result.get("merit_name")),
        ),
    )

    result["organizing_entity"] = first_non_empty_text(
        result.get("organizing_entity"),
        result.get("institution_name"),
        heuristic_result.get("organizing_entity"),
        heuristic_result.get("institution_name"),
        extracted_institution_name,
    )
    if not clean_text(result.get("institution_name")):
        result["institution_name"] = first_non_empty_text(result["organizing_entity"], extracted_institution_name)

    result["professional_category"] = first_non_empty_text(
        result.get("professional_category"),
        heuristic_result.get("professional_category"),
        extract_professional_category(
            document_text,
            merit_name=first_non_empty_text(result.get("merit_name"), heuristic_result.get("merit_name")),
            institution_name=result.get("institution_name"),
            description=first_non_empty_text(result.get("description"), result.get("document_description")),
        ),
    )

    result["center_name"] = first_non_empty_text(
        result.get("center_name"),
        heuristic_result.get("center_name"),
        extracted_institution_name,
        extract_center_name(
            result.get("institution_name"),
            result.get("center_search_text"),
            heuristic_result.get("center_search_text"),
        ),
    )
    if not clean_text(result.get("center_search_text")):
        result["center_search_text"] = shorten_center_name(
            result.get("center_name") or result.get("institution_name") or extracted_institution_name
        )

    result["center_code"] = first_non_empty_text(
        result.get("center_code"),
        heuristic_result.get("center_code"),
        extract_center_code(document_text),
    )

    explicit_lookup = first_known_bool(
        result.get("center_code_lookup_required"),
        heuristic_result.get("center_code_lookup_required"),
    )
    if explicit_lookup is not None:
        result["center_code_lookup_required"] = explicit_lookup
    elif result.get("center_code"):
        result["center_code_lookup_required"] = False
    elif result.get("center_name"):
        result["center_code_lookup_required"] = True
    else:
        result["center_code_lookup_required"] = None

    result["accreditation_cfc"] = derive_accreditation_cfc(result, heuristic_result, document_text=document_text)
    result["accreditation_type"] = derive_accreditation_type(result, heuristic_result, document_text=document_text)

    result["description"] = first_non_empty_text(
        result.get("description"),
        result.get("document_description"),
        heuristic_result.get("description"),
        heuristic_result.get("document_description"),
    )
    if not clean_text(result.get("document_description")):
        result["document_description"] = result["description"]

    result["document_justification_only"] = first_known_bool(
        result.get("document_justification_only"),
        heuristic_result.get("document_justification_only"),
        False,
    )

    result["signature_or_stamp_present"] = first_known_bool(
        result.get("signature_or_stamp_present"),
        heuristic_result.get("signature_or_stamp_present"),
        detect_signature_or_stamp_present(document_text),
    )
    result["front_and_back_complete"] = first_known_bool(
        result.get("front_and_back_complete"),
        heuristic_result.get("front_and_back_complete"),
        detect_front_and_back_complete(document_text),
    )
    result["cfc_logo_present"] = first_known_bool(
        result.get("cfc_logo_present"),
        heuristic_result.get("cfc_logo_present"),
        detect_cfc_logo_present(document_text),
    )
    result["is_provisional_document"] = first_known_bool(
        result.get("is_provisional_document"),
        heuristic_result.get("is_provisional_document"),
        detect_provisional_document(document_text),
    )
    result["is_private_center"] = first_known_bool(
        result.get("is_private_center"),
        heuristic_result.get("is_private_center"),
        detect_private_center(document_text, institution_name=result.get("institution_name")),
    )

    result["required_documents"] = derive_required_documents(result, heuristic_result)
    result["alerts"] = build_rule_alerts(result, heuristic_result)

    evidence = result.get("field_evidence")
    if not isinstance(evidence, dict):
        evidence = {}

    result["field_evidence"] = build_field_evidence(
        {**result, "field_evidence": evidence},
        heuristic_result,
        document_text=document_text,
    )

    return result


def normalize_accrediting_body_value(value):
    exact_match = normalize_choice_from_options(value, ACCREDITING_BODY_VALUES)
    if exact_match:
        return exact_match

    normalized = normalize_text(value)
    if not normalized:
        return ""

    if any(token in normalized for token in ("secretaria general de salud publica", "autonomica", "junta de andalucia", "i+d+i")):
        return "Acreditado por la ComisiÃ³n de FormaciÃ³n Continuada AutonÃ³mica"
    if any(token in normalized for token in ("sistema nacional de salud", "del sns", "sns")):
        return "Acreditado por la ComisiÃ³n de FormaciÃ³n Continuada del SNS"
    if any(token in normalized for token in ("consejo internacional de enfermeria", "international council of nurses")):
        return "Acreditado por el Consejo Internacional de EnfermerÃ­a"
    if "eaccme" in normalized or "european accreditation council" in normalized:
        return "Acreditado por European Accreditation Council For CME (EACCME)"
    if "american medical association" in normalized or re.search(r"\bama\b", normalized):
        return "Acreditado por  American Medical Association (AMA)"
    if "royal college of physicians and surgeons of canada" in normalized:
        return "Acreditado por Royal College of Physicians and Surgeons of Canada"

    return ""


def parse_numeric_date(value):
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def parse_written_date(value, default_year=None):
    normalized_value = normalize_text(value)
    match = re.search(
        r"(\d{1,2})\s+de\s+([a-zA-Z]+)(?:\s+de\s+(\d{4}))?",
        normalized_value,
    )
    if not match:
        return None

    day = int(match.group(1))
    month = SPANISH_MONTHS.get(match.group(2))
    year_text = match.group(3)
    if year_text:
        year = int(year_text)
    elif default_year:
        year = int(default_year)
    else:
        return None
    if not month:
        return None

    return dt.date(year, month, day)


def format_date(value):
    return value.strftime("%d/%m/%Y") if value else ""


def contains_keyword(text, keyword):
    normalized_text = normalize_text(text)
    normalized_keyword = normalize_text(keyword)
    if not normalized_text or not normalized_keyword:
        return False

    if len(normalized_keyword) <= 3 and re.fullmatch(r"[a-z0-9]+", normalized_keyword):
        return re.search(rf"\b{re.escape(normalized_keyword)}\b", normalized_text) is not None

    return normalized_keyword in normalized_text


def extract_date_range(document_text):
    patterns = [
        r"realizado del\s+(\d{1,2}/\d{1,2}/\d{4})\s+al\s+(\d{1,2}/\d{1,2}/\d{4})",
        r"realizado del\s+(\d{1,2}\s+de\s+[A-Za-z]+)\s+al\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})",
        r"fecha inicio estudios:\s*(\d{1,2}/\d{1,2}/\d{4}).*?fecha fin estudios:\s*(\d{1,2}/\d{1,2}/\d{4})",
        r"celebrado del\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})\s+al\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})",
        r"celebrado del\s+(\d{1,2}\s+de\s+[A-Za-z]+)\s+al\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})",
        r"realizado entre el\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})\s+y el\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})",
        r"realizado entre el\s+(\d{1,2}\s+de\s+[A-Za-z]+)\s+y el\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})",
        r"del\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})\s+al\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})",
        r"del\s+(\d{1,2}\s+de\s+[A-Za-z]+)\s+al\s+(\d{1,2}\s+de\s+[A-Za-z]+\s+de\s+\d{4})",
    ]

    for pattern in patterns:
        match = re.search(pattern, document_text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue

        start_raw, end_raw = match.group(1), match.group(2)
        end_date = parse_numeric_date(end_raw) or parse_written_date(end_raw)
        start_date = parse_numeric_date(start_raw) or parse_written_date(
            start_raw,
            default_year=end_date.year if end_date else None,
        )
        if start_date and end_date:
            return format_date(start_date), format_date(end_date), end_date.year

    all_numeric_dates = []
    for value in re.findall(r"\b\d{2}/\d{2}/\d{4}\b", document_text):
        parsed = parse_numeric_date(value)
        if parsed:
            all_numeric_dates.append(parsed)

    if all_numeric_dates:
        start_date = min(all_numeric_dates)
        end_date = max(all_numeric_dates)
        return format_date(start_date), format_date(end_date), end_date.year

    return "", "", 0


def extract_merit_year(document_text):
    normalized = normalize_text(document_text)
    patterns = [
        r"en su edicion\s+(20\d{2}|19\d{2})",
        r"edicion\s+(20\d{2}|19\d{2})",
        r"curso academico\s+(20\d{2}|19\d{2})(?:/\d{2,4})?",
        r"promocion\s+(20\d{2}|19\d{2})",
        r"convocatoria\s+(20\d{2}|19\d{2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))

    return 0


def parse_result_year(value):
    if value in (None, "", 0):
        return 0

    text = str(value).strip()
    if re.fullmatch(r"\d{4}", text):
        return int(text)

    parsed = parse_numeric_date(text) or parse_written_date(text)
    if parsed:
        return parsed.year

    return 0


def repair_result_dates(result):
    year = parse_result_year(result.get("year"))
    if not year:
        year = parse_result_year(result.get("end_date"))

    if year and not parse_result_year(result.get("year")):
        result["year"] = year

    return result


def extract_number(document_text, patterns):
    search_texts = [repair_mojibake_text(document_text), str(document_text or "")]

    def parse_number_text(raw_value):
        text = re.sub(r"\s+", "", str(raw_value or ""))
        if not text:
            raise ValueError("Empty numeric value")

        if "," in text and "." in text:
            if text.rfind(",") > text.rfind("."):
                text = text.replace(".", "").replace(",", ".")
            else:
                text = text.replace(",", "")
        elif "," in text:
            left, right = text.rsplit(",", 1)
            text = f"{left}.{right}" if len(right) <= 2 else text.replace(",", "")
        elif "." in text and text.count(".") == 1:
            left, right = text.rsplit(".", 1)
            if len(right) == 3 and left.replace("-", "").isdigit():
                text = left + right
        elif "." in text and text.count(".") > 1:
            text = text.replace(".", "")

        value = float(text)
        return int(value) if value.is_integer() else value

    for search_text in search_texts:
        if not search_text:
            continue
        for pattern in patterns:
            match = re.search(pattern, search_text, flags=re.IGNORECASE)
            if not match:
                continue

            try:
                return parse_number_text(match.group(1))
            except ValueError:
                continue

    return 0


def extract_hours(document_text):
    return extract_number(
        document_text,
        [
            r"duraci[oó]n de\s+(\d+(?:[.,]\d+)?)\s*horas",
            r"carga lectiva:\s*(\d+(?:[.,]\d+)?)\s*horas",
            r"(\d+(?:[.,]\d+)?)\s*horas\s*\(",
            r"(\d+(?:[.,]\d+)?)\s*horas\b",
        ],
    )


def extract_credits(document_text):
    return extract_number(
        document_text,
        [
            r"(\d+(?:[.,]\d+)?)\s*cr[eé]ditos?\s*ects",
            r"(\d+(?:[.,]\d+)?)\s*cr[eé]ditos?\b",
            r"carga lectiva:\s*\d+(?:[.,]\d+)?\s*horas\s*-\s*(\d+(?:[.,]\d+)?)\s*cr[eé]ditos?",
        ],
    )


def extract_course_code(document_text):
    patterns = [
        r"actividad docente con\s*n[Âºo]\s*de expediente\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-\/]+)",
        r"n[Âºo]\s*de expediente\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-\/]+)",
        r"expediente\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-\/]+)",
        r"resoluci[oÃ³]n[^.\n]*?\s+n[Âºo]\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-\/]+)",
        r"c[oÃ³]digo(?:\s+del\s+curso|\s+curso)\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-\/]+)",
        r"reg\.doc\.\s*([A-Za-z0-9\-\/]+)",
        r"csv:\s*([A-Za-z0-9=\/+\-]+)",
        r"n[Âºo]\s*registro:\s*([A-Za-z0-9\-\/]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, document_text, flags=re.IGNORECASE)
        if match:
            return clean_text(match.group(1))

    return ""


def contains_any_keyword(text, keywords):
    return any(contains_keyword(text, keyword) for keyword in keywords)


def extract_scope(document_text, institution_name=""):
    normalized = normalize_text(f"{institution_name} {document_text}")
    if not normalized:
        return ""

    if "ambito nacional" in normalized or "ambito estatal" in normalized or "national scope" in normalized:
        return "Nacional"
    if "ambito autonomico" in normalized or "regional scope" in normalized:
        return "AutonÃ³mico"

    if contains_any_keyword(normalized, ANDALUSIA_KEYWORDS):
        return "AutonÃ³mico"

    if contains_any_keyword(normalized, EXTRA_EU_COUNTRY_KEYWORDS):
        return "Extracomunitario"

    if contains_any_keyword(normalized, EU_COUNTRY_KEYWORDS):
        return "Comunitario"

    spain_markers = ("espana", "spain")
    if contains_any_keyword(normalized, spain_markers):
        return "Nacional"

    return ""


def extract_delivery_method(document_text):
    normalized = normalize_text(document_text)

    if "mooc" in normalized or "cursos online masivos y abiertos" in normalized:
        return "Cursos online masivos y abiertos (OM)"

    online_markers = (
        "e-learning",
        "elearning",
        "online",
        "on line",
        "virtual",
        "campus virtual",
        "aula virtual",
        "telematica",
        "telematico",
    )
    distance_markers = (
        "a distancia",
        "distancia",
        "no presencial",
    )

    has_online = contains_any_keyword(normalized, online_markers)
    has_distance = contains_any_keyword(normalized, distance_markers)
    has_presential = "presencial" in normalized

    if "semipresencial" in normalized or "mixta" in normalized or (has_presential and (has_online or has_distance)):
        return "Semipresencial (S)"
    if has_online:
        return "A distancia modalidad e-learning (V)"
    if has_presential:
        return "FormaciÃ³n Presencial (P)"
    if has_distance:
        return "A distancia (D)"

    return normalize_delivery_method_value(document_text)


def extract_university_diploma_type(document_text, merit_name=""):
    normalized = normalize_text(f"{merit_name} {document_text}")

    if "diploma de especializacion" in normalized:
        return "TÃ­tulo Propio Diploma de EspecializaciÃ³n"
    if "experto universitario" in normalized:
        return "TÃ­tulo Propio Experto Universitario"
    if "especialista universitario" in normalized:
        return "TÃ­tulo Propio Especialista Universitario"
    if "extension universitaria" in normalized:
        return "Cursos, Diplomas o Certificaciones de ExtensiÃ³n Universitaria"
    if any(
        marker in normalized
        for marker in ("curso universitario", "certificado universitario", "diploma universitario")
    ):
        return "Cursos, Diplomas o Certificaciones de ExtensiÃ³n Universitaria"
    if "master universitario no grado academico" in normalized or "master de formacion permanente" in normalized:
        return "Master Universitario No Grado AcadÃ©mico"
    if "master titulo propio" in normalized or ("titulo propio" in normalized and "master" in normalized):
        return "Master TÃ­tulo Propio"
    if "master" in normalized:
        return "Master TÃ­tulo Propio"
    if "titulo propio" in normalized:
        return "TÃ­tulo Propio Universitario distinto a los Anteriores"

    return ""


def extract_training_type(document_text, route="unknown", merit_name=""):
    if route == "diplomas_titulos_propios_universitarios":
        return extract_university_diploma_type(document_text, merit_name=merit_name)

    normalized = normalize_text(f"{merit_name} {document_text}")
    return normalize_training_type_value(normalized, route=route)


def extract_credits_or_hours(document_text, credits=0, hours=0):
    if credits not in (None, "", 0):
        return normalize_credits_or_hours_value("CrÃ©ditos")
    if hours not in (None, "", 0):
        return normalize_credits_or_hours_value("Horas")

    normalized = normalize_text(document_text)
    if "ects" in normalized or "creditos" in normalized:
        return normalize_credits_or_hours_value("CrÃ©ditos")
    if "horas" in normalized:
        return normalize_credits_or_hours_value("Horas")

    return ""


def has_admin_nonprofit_recognition(document_text):
    normalized = normalize_text(document_text)
    recognition_markers = (
        "reconocimiento de interes sanitario",
        "actividad con reconocimiento de interes sanitario",
        "reconocida de interes sanitario",
    )
    return any(marker in normalized for marker in recognition_markers)


def contains_route_markers(normalized_text, markers):
    return any(marker in normalized_text for marker in markers)


def derive_supported_vec_route(document_text, merit_name="", institution_name=""):
    normalized = normalize_text(f"{merit_name} {institution_name} {document_text}")
    if not normalized:
        return "unknown"

    if "servicio andaluz de salud" in normalized and "prestado los siguientes servicios" in normalized:
        return "experiencia_sas"

    if contains_route_markers(normalized, SUPPORTED_ROUTE_MARKERS["estancias_formativas"]):
        return "estancias_formativas"

    if contains_route_markers(normalized, SUPPORTED_ROUTE_MARKERS["diplomas_titulos_propios_universitarios"]):
        return "diplomas_titulos_propios_universitarios"

    if contains_route_markers(normalized, UNSUPPORTED_ACADEMIC_ROUTE_MARKERS):
        return "manual_review"

    if (
        contains_route_markers(normalized, SUPPORTED_ROUTE_MARKERS["formacion_continuada"])
        or has_admin_nonprofit_recognition(document_text)
    ):
        return "formacion_continuada"

    return "unknown"


def canonicalize_duration_selection(credits, hours):
    canonical_credits = None if credits in (None, "", 0) else credits
    canonical_hours = None if hours in (None, "", 0) else hours

    if canonical_credits not in (None, "", 0):
        return None, canonical_credits, normalize_credits_or_hours_value("CrÃ©ditos")
    if canonical_hours not in (None, "", 0):
        return canonical_hours, None, normalize_credits_or_hours_value("Horas")
    return None, None, ""


def format_duration_hint(credits, hours):
    canonical_hours, canonical_credits, canonical_units = canonicalize_duration_selection(credits, hours)
    normalized_units = normalize_text(canonical_units)

    if normalized_units.startswith("credit") and canonical_credits not in (None, "", 0):
        return f"Créditos: {canonical_credits}"
    if normalized_units.startswith("hora") and canonical_hours not in (None, "", 0):
        return f"Horas: {canonical_hours}"
    return ""


def extract_accrediting_body(document_text):
    option_match = normalize_accrediting_body_value(document_text)
    if option_match:
        return option_match

    if has_admin_nonprofit_recognition(document_text):
        return ADMIN_NONPROFIT_ACCREDITATION_TYPE

    patterns = [
        r"actividad acreditada por\s+([^.]+?)(?:\s+y\s+organizado|\.)",
        r"acreditada por\s+([^.]+?)(?:\s+y\s+organizado|\.)",
        r"comisi[oÃ³]n de formaci[oÃ³]n continuada[^.,\n]*",
    ]

    for pattern in patterns:
        match = re.search(pattern, document_text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        value = match.group(0) if match.lastindex is None else match.group(1)
        normalized_value = normalize_accrediting_body_value(value)
        if normalized_value:
            return normalized_value

    return ""


def extract_accredited_choice(document_text):
    if extract_accrediting_body(document_text):
        return "si"

    normalized = normalize_text(document_text)
    positive_markers = (
        "actividad acreditada",
        "acreditada por",
        "comision de formacion continuada",
        "creditos",
    )
    if any(marker in normalized for marker in positive_markers):
        return "si"
    if has_admin_nonprofit_recognition(document_text):
        return "si"
    return ""


def infer_center_result_text(institution_name, document_text):
    normalized_institution = normalize_text(institution_name)
    normalized_document = normalize_text(document_text)

    if "servicio andaluz de salud" in normalized_institution or "servicio andaluz de salud" in normalized_document:
        return "11-CENTROS O ENTIDADES SANITARIAS SERVICIO ANDALUZ DE SALUD"
    if "colegio oficial" in normalized_institution or normalized_institution.startswith("colegio "):
        return "13-COLEGIO OFICIAL"
    if "asociacion" in normalized_institution or "federacion" in normalized_institution:
        return "02-AsociaciÃ³n, FederaciÃ³n o asociacion de asociaciones"
    return ""


def extract_document_description(merit_name, pdf_path):
    return clean_text(merit_name) or clean_text(pdf_path.stem)


def extract_document_justification_only(document_text):
    normalized = normalize_text(document_text)
    markers = (
        "justificante de haber solicitado",
        "solicitado la certificacion",
        "solicitud de certificacion",
        "resguardo de solicitud",
    )
    return any(marker in normalized for marker in markers)


def extract_institution_name(document_text):
    search_texts = [repair_mojibake_text(document_text), str(document_text or "")]
    patterns = [
        r"actividad docente,\s*organizad[oa]\s+por\s+(.+?)(?:,?\s+celebrad[oa]|,?\s+realizad[oa]|,|\.)",
        r"organizad[oa]\s+por\s+(.+?)(?:,?\s+celebrad[oa]|,?\s+realizad[oa]|,|\.)",
        r"el rector de\s+(.+?)(?:\s+considerando|\s+expide|\n|$)",
        r"titulaci[oó]n propia de\s+(.+?)(?:\s{2,}|\n|$)",
        r"documento firmado por\s+(.+?)(?:\s+-\s+[A-Z0-9]{6,}|,|\.)",
        r"firmad[oa]\s+digitalmente\s+por\s+(.+?)(?:\s+\(|,|\.)",
        r"\bo=([A-ZÁÉÍÓÚÑ0-9 .,&\-]{6,}?)(?:,|\)|$)",
        r"universidad [A-ZÁÉÍÓÚÑA-Za-záéíóúñ .,&\-]{3,}",
        r"universit[aà](?: degli studi)? [A-ZÁÉÍÓÚÑA-Za-záéíóúñ .,&\-]{3,}",
        r"Servicio Andaluz de Salud",
        r"ACMA\s+Asociaci[oÃ³]n para la Formaci[oÃ³]n y EnseÃ±anza Profesional",
        r"NETTO\s+Asociaci[oÃ³]n para la Formaci[oÃ³]n y EnseÃ±anza Profesional",
    ]

    for search_text in search_texts:
        if not search_text:
            continue

        for pattern in patterns:
            match = re.search(pattern, search_text, flags=re.IGNORECASE | re.DOTALL)
            if not match:
                continue
            value = match.group(0) if match.lastindex is None else match.group(1)
            value = re.sub(r"^(?:la|el)\s+", "", str(value or ""), flags=re.IGNORECASE).strip()
            value = re.sub(r"\s+-\s+[A-Z0-9]{6,}$", "", value).strip()
            cleaned = clean_text(value)
            if cleaned:
                return cleaned

    return ""


def shorten_center_name(institution_name):
    if not institution_name:
        return ""
    if "ACMA" in institution_name.upper():
        return "ACMA"
    if "Servicio Andaluz de Salud" in institution_name:
        return "SAS"
    if re.search(r"\buniversidad\b", institution_name, flags=re.IGNORECASE):
        return "Universidad"
    return institution_name


def extract_merit_name(document_text, pdf_path):
    search_texts = [repair_mojibake_text(document_text), str(document_text or "")]
    patterns = [
        r"el curso:\s*[\"â€œâ€]?\s*(.+?)\s*[\"â€]?\s*realizado del",
        r"ha asistido.*?\bal curso\s+([^,\n]+),\s*c[oÃ³]digo",
        r"\bal curso\s+([^,\n]+),\s*c[oÃ³]digo",
        r"ha superado con\s+total\s+aprovechamiento.*?el curso:\s*[\"â€œ]?([^\"\n]+)",
        r"ha superado con [^,\n]+ el curso\s+([^,\n]+),\s*modalidad",
        r"expide el presente t[íi]tulo de\s+(.+?)\s+dirigido a",
        r"expide el presente t[íi]tulo de\s+(.+?)\s+(?:realizado entre|cursado entre|con una asignaci[oó]n|por haber|dado en|$)",
        r"certificado universitario\s+en\s+(.+?)\s+en [A-ZÃÃ‰ÃÃ“ÃšÃ‘a-zÃ¡Ã©Ã­Ã³ÃºÃ±]+ a",
        r"(?:experto universitario|especialista universitario|certificado universitario|curso universitario)\s+en\s+(.+?)(?:\s+dirigido a|\s+realizado|\s+cursado|\s+con una asignaci[oó]n|\s+por haber|$)",
        r"master(?: universitario| de formacion permanente| titulo propio| t[íi]tulo propio)?\s+(?:en|di)\s+(.+?)(?:\s+dirigido a|\s+realizado|\s+cursado|\s+con una asignaci[oó]n|\s+por haber|\s+anno accademico|$)",
    ]

    for search_text in search_texts:
        if not search_text:
            continue

        for pattern in patterns:
            match = re.search(pattern, search_text, flags=re.IGNORECASE | re.DOTALL)
            if not match:
                continue
            cleaned = clean_text(match.group(1))
            if cleaned:
                return cleaned

    name = pdf_path.stem
    name = re.sub(r"^[0-9a-f\-]+-documentos-", "", name, flags=re.IGNORECASE)
    name = re.sub(r"-[0-9a-f]{6,}$", "", name, flags=re.IGNORECASE)
    return clean_text(name)


def classify_with_heuristics(document_text, pdf_path):
    normalized = normalize_text(document_text)
    result = blank_result()

    if "servicio andaluz de salud" in normalized and "prestado los siguientes servicios" in normalized:
        start_date, end_date, year = extract_date_range(document_text)
        result.update(
            {
                "vec_route": "experiencia_sas",
                "merit_name": "Experiencia SAS",
                "merit_type": derive_merit_type("experiencia_sas", merit_name="Experiencia SAS"),
                "organizing_entity": "Servicio Andaluz de Salud",
                "institution_name": "Servicio Andaluz de Salud",
                "center_name": "Servicio Andaluz de Salud",
                "center_search_text": "SAS",
                "start_date": start_date,
                "end_date": end_date,
                "center_code_lookup_required": True,
                "description": "Experiencia SAS",
                "year": year,
                "confidence": 0.98,
                "reasoning_summary": "Certificado de servicios prestados del SAS.",
            }
        )
        heuristic_only = enrich_result_with_rule_fields(result, result, document_text=document_text)
        return repair_result_dates(
            finalize_result_for_vec(
                heuristic_only,
                heuristic_only,
                document_text=document_text,
                pdf_path=pdf_path,
            )
        )

    start_date, end_date, year = extract_date_range(document_text)
    merit_year = extract_merit_year(document_text)
    if merit_year and not year:
        year = merit_year

    institution_name = extract_institution_name(document_text)
    merit_name = extract_merit_name(document_text, pdf_path)
    route = derive_supported_vec_route(
        document_text,
        merit_name=merit_name,
        institution_name=institution_name,
    )
    credits = extract_credits(document_text)
    hours = extract_hours(document_text)
    hours, credits, credits_or_hours = canonicalize_duration_selection(credits, hours)
    document_description = extract_document_description(merit_name, pdf_path)

    result.update(
        {
            "vec_route": route,
            "merit_name": merit_name,
            "merit_type": derive_merit_type(
                route,
                training_type=extract_training_type(document_text, route=route, merit_name=merit_name),
                merit_name=merit_name,
            ),
            "professional_category": extract_professional_category(
                document_text,
                merit_name=merit_name,
                institution_name=institution_name,
                description=document_description,
            ),
            "organizing_entity": institution_name,
            "institution_name": institution_name,
            "center_name": extract_center_name(institution_name),
            "center_search_text": shorten_center_name(institution_name),
            "center_result_text": infer_center_result_text(institution_name, document_text),
            "center_code": extract_center_code(document_text),
            "start_date": start_date,
            "end_date": end_date,
            "scope": extract_scope(document_text, institution_name=institution_name),
            "training_type": extract_training_type(document_text, route=route, merit_name=merit_name),
            "delivery_method": extract_delivery_method(document_text),
            "accredited_choice": extract_accredited_choice(document_text),
            "accredited_received": extract_accrediting_body(document_text),
            "accreditation_cfc": True if "comision de formacion continuada" in normalized else None,
            "course_code": extract_course_code(document_text),
            "credits_or_hours": credits_or_hours or extract_credits_or_hours(document_text, credits=credits, hours=hours),
            "hours": hours,
            "credits": credits,
            "document_description": document_description,
            "description": document_description,
            "document_justification_only": extract_document_justification_only(document_text),
            "signature_or_stamp_present": detect_signature_or_stamp_present(document_text),
            "front_and_back_complete": detect_front_and_back_complete(document_text),
            "cfc_logo_present": detect_cfc_logo_present(document_text),
            "is_provisional_document": detect_provisional_document(document_text),
            "is_private_center": detect_private_center(document_text, institution_name=institution_name),
            "year": year,
            "confidence": 0.9 if route != "unknown" else 0.5,
            "reasoning_summary": "Clasificacion inferida desde el texto extraido del PDF.",
        }
    )
    heuristic_only = enrich_result_with_rule_fields(result, result, document_text=document_text)
    return repair_result_dates(
        finalize_result_for_vec(
            heuristic_only,
            heuristic_only,
            document_text=document_text,
            pdf_path=pdf_path,
        )
    )


def build_pdf_fact_hints(document_text, pdf_path):
    start_date, end_date, year = extract_date_range(document_text)
    merit_name = clean_text(extract_merit_name(document_text, pdf_path))
    institution_name = clean_text(extract_institution_name(document_text))
    document_description = extract_document_description(merit_name, pdf_path)
    professional_category = clean_text(
        extract_professional_category(
            document_text,
            merit_name=merit_name,
            institution_name=institution_name,
            description=document_description,
        )
    )

    hours, credits, credits_or_hours = canonicalize_duration_selection(
        extract_credits(document_text),
        extract_hours(document_text),
    )

    return {
        "merit_name": merit_name,
        "institution_name": institution_name,
        "start_date": clean_text(start_date),
        "end_date": clean_text(end_date),
        "year": year,
        "professional_category": professional_category,
        "course_code": clean_text(extract_course_code(document_text)),
        "credits": credits,
        "hours": hours,
        "credits_or_hours": credits_or_hours,
    }


def normalize_ai_result(ai_result):
    if not isinstance(ai_result, dict):
        return {}

    normalized = {}
    for key, value in ai_result.items():
        if key in BOOLEAN_RESULT_FIELDS:
            normalized[key] = coerce_optional_bool(value)
        elif key in LIST_RESULT_FIELDS:
            normalized[key] = clean_string_list(value)
        elif isinstance(value, str):
            normalized[key] = clean_text(value)
        else:
            normalized[key] = value

    evidence = normalized.get("field_evidence")
    if isinstance(evidence, dict):
        normalized["field_evidence"] = {
            str(key): clean_text(value) if isinstance(value, str) else value
            for key, value in evidence.items()
        }

    return normalized


def has_result_duration_data(result):
    return bool(
        clean_text(result.get("credits_or_hours"))
        or result.get("hours") not in (None, "", 0)
        or result.get("credits") not in (None, "", 0)
    )


def has_result_dates_for_route(result, route):
    if clean_text(result.get("start_date")) and clean_text(result.get("end_date")):
        return True

    if route == "diplomas_titulos_propios_universitarios" and parse_result_year(result.get("year")):
        return True

    return False


def count_link_like_markers(document_text):
    normalized = normalize_text(document_text)
    if not normalized:
        return 0

    hits = sum(1 for marker in LINK_ONLY_MARKERS if marker in normalized)
    raw_text = str(document_text or "").lower()
    if "http://" in raw_text or "https://" in raw_text:
        hits += 1

    return hits


def merit_name_looks_filename_like(merit_name, pdf_path):
    if not merit_name or not pdf_path:
        return False

    candidate_name = Path(pdf_path).stem
    candidate_name = re.sub(r"^[0-9a-f\-]+-documentos-", "", candidate_name, flags=re.IGNORECASE)
    candidate_name = re.sub(r"-[0-9a-f]{6,}$", "", candidate_name, flags=re.IGNORECASE)

    merit_normalized = normalize_text(merit_name)
    candidate_normalized = normalize_text(candidate_name)
    return bool(merit_normalized and candidate_normalized and merit_normalized == candidate_normalized)


def format_completion_field_names(fields):
    return ", ".join(CLASSIFICATION_COMPLETENESS_FIELD_LABELS.get(field, field) for field in fields)


def assess_classification_completeness(result, document_text="", pdf_path=None):
    route = str(result.get("vec_route") or "").strip()
    missing_fields = []

    if route in ("", "unknown", "manual_review"):
        missing_fields.append("vec_route")
    if not clean_text(result.get("merit_name")):
        missing_fields.append("merit_name")
    if not clean_text(result.get("institution_name")):
        missing_fields.append("institution_name")
    if not has_result_dates_for_route(result, route):
        missing_fields.append("dates")
    if route in TRAINING_ROUTE_VALUES and not clean_text(result.get("training_type")):
        missing_fields.append("training_type")
    if route == "formacion_continuada" and not clean_text(result.get("delivery_method")):
        missing_fields.append("delivery_method")
    if route in TRAINING_ROUTE_VALUES and not has_result_duration_data(result):
        missing_fields.append("duration")
    if route == "formacion_continuada" and not clean_text(result.get("accredited_choice")):
        missing_fields.append("accredited_choice")
    if not clean_text(result.get("document_description")):
        missing_fields.append("document_description")

    course_code_missing = route in TRAINING_ROUTE_VALUES and not clean_text(result.get("course_code"))
    link_like_marker_count = count_link_like_markers(document_text)
    filename_like_merit_name = merit_name_looks_filename_like(result.get("merit_name"), pdf_path)
    non_route_missing_count = len([field for field in missing_fields if field != "vec_route"])

    possible_link_only_document = bool(
        link_like_marker_count >= 2
        and (
            course_code_missing
            or "dates" in missing_fields
            or filename_like_merit_name
        )
    )

    blocking_fields = list(missing_fields)
    if course_code_missing and (possible_link_only_document or non_route_missing_count >= 2):
        blocking_fields.append("course_code")

    should_block = bool(
        route in ("", "unknown", "manual_review")
        or possible_link_only_document
        or non_route_missing_count >= 3
        or (course_code_missing and (link_like_marker_count >= 1 or non_route_missing_count >= 2))
    )

    return {
        "should_block": should_block,
        "blocking_fields": dedupe_preserve_order(blocking_fields),
        "course_code_missing": course_code_missing,
        "possible_link_only_document": possible_link_only_document,
    }


def apply_classification_completeness_gate(result, heuristic_result, document_text="", pdf_path=None):
    assessment = assess_classification_completeness(result, document_text=document_text, pdf_path=pdf_path)
    alerts = clean_string_list(result.get("alerts"))

    if assessment["possible_link_only_document"]:
        alerts.append("POSIBLE DOCUMENTO SOLO DE ENLACE O VALIDACION. NO CLASIFICAR NI SUBIR.")
    if assessment["course_code_missing"] and assessment["should_block"]:
        alerts.append("FALTA CODIGO DE CURSO/EXPEDIENTE EN EL PDF.")
    if assessment["blocking_fields"]:
        alerts.append(
            "CLASIFICACION INCOMPLETA: faltan campos criticos para automatizar con seguridad: "
            + format_completion_field_names(assessment["blocking_fields"])
        )

    result["alerts"] = dedupe_preserve_order(alerts)

    if assessment["should_block"]:
        result["vec_route"] = "manual_review"
        try:
            result["confidence"] = min(float(result.get("confidence") or 0.0), 0.2)
        except (TypeError, ValueError):
            result["confidence"] = 0.0
        result["reasoning_summary"] = clean_text(
            first_non_empty_text(result.get("reasoning_summary"), heuristic_result.get("reasoning_summary"))
            + " Clasificacion detenida por documento incompleto o posible enlace/validacion."
        )

    return result


def finalize_result_for_vec(result, heuristic_result, document_text="", pdf_path=None):
    route = str(result.get("vec_route") or heuristic_result.get("vec_route") or "").strip()

    training_type = normalize_training_type_value(result.get("training_type"), route=route)
    if not training_type:
        training_type = normalize_training_type_value(heuristic_result.get("training_type"), route=route)
    result["training_type"] = training_type

    delivery_method = normalize_delivery_method_value(result.get("delivery_method"))
    if not delivery_method:
        delivery_method = normalize_delivery_method_value(heuristic_result.get("delivery_method"))
    if not delivery_method and route == "formacion_continuada":
        delivery_method = "A distancia (D)"
    result["delivery_method"] = delivery_method

    accredited_choice = normalize_yes_no_value(result.get("accredited_choice"))
    if not accredited_choice:
        accredited_choice = normalize_yes_no_value(heuristic_result.get("accredited_choice"))
    if not accredited_choice and (
        normalize_accrediting_body_value(result.get("accredited_received"))
        or normalize_accrediting_body_value(heuristic_result.get("accredited_received"))
        or result.get("credits") not in (None, "", 0)
        or heuristic_result.get("credits") not in (None, "", 0)
    ):
        accredited_choice = "si"
    result["accredited_choice"] = accredited_choice

    accredited_received = normalize_accrediting_body_value(result.get("accredited_received"))
    if not accredited_received:
        accredited_received = normalize_accrediting_body_value(heuristic_result.get("accredited_received"))
    if not accredited_received and (
        derive_accreditation_cfc(result, heuristic_result) is True
        or derive_accreditation_type(result, heuristic_result) == SNS_ACCREDITATION_TYPE
    ):
        accredited_received = "Acreditado por la ComisiÃ³n de FormaciÃ³n Continuada del SNS"
    result["accredited_received"] = accredited_received

    canonical_hours, canonical_credits, canonical_units = canonicalize_duration_selection(
        result.get("credits", 0),
        result.get("hours", 0),
    )
    if canonical_hours in (None, "", 0) and canonical_credits in (None, "", 0):
        canonical_hours, canonical_credits, canonical_units = canonicalize_duration_selection(
            heuristic_result.get("credits", 0),
            heuristic_result.get("hours", 0),
        )
    result["hours"] = canonical_hours
    result["credits"] = canonical_credits

    credits_or_hours = normalize_credits_or_hours_value(result.get("credits_or_hours"))
    if not credits_or_hours:
        credits_or_hours = normalize_credits_or_hours_value(heuristic_result.get("credits_or_hours"))
    if not credits_or_hours:
        credits_or_hours = extract_credits_or_hours(
            "",
            credits=result.get("credits", 0) or heuristic_result.get("credits", 0),
            hours=result.get("hours", 0) or heuristic_result.get("hours", 0),
        )
    if canonical_units:
        credits_or_hours = canonical_units
    result["credits_or_hours"] = credits_or_hours

    if not str(result.get("course_code") or "").strip():
        result["course_code"] = clean_text(heuristic_result.get("course_code"))

    if result["accredited_choice"] == "no":
        result["accredited_received"] = ""

    for key in (
        "vec_route",
        "merit_name",
        "merit_type",
        "professional_category",
        "organizing_entity",
        "institution_name",
        "center_name",
        "center_search_text",
        "center_result_text",
        "center_code",
        "start_date",
        "end_date",
        "scope",
        "training_type",
        "delivery_method",
        "accredited_choice",
        "accredited_received",
        "accreditation_type",
        "course_code",
        "credits_or_hours",
        "document_description",
        "description",
        "reasoning_summary",
    ):
        result[key] = clean_text(result.get(key))

    enriched = enrich_result_with_rule_fields(result, heuristic_result, document_text=document_text)
    enriched = apply_classification_completeness_gate(
        enriched,
        heuristic_result,
        document_text=document_text,
        pdf_path=pdf_path,
    )
    enriched["vec_step_payload"] = build_vec_step_payload(enriched)
    return enriched


def text_values_conflict(left, right):
    left_normalized = normalize_text(left)
    right_normalized = normalize_text(right)
    return bool(left_normalized and right_normalized and left_normalized != right_normalized)


def numeric_values_conflict(left, right):
    if left in (None, "", 0) or right in (None, "", 0):
        return False

    try:
        return abs(float(left) - float(right)) > 1e-9
    except (TypeError, ValueError):
        return str(left).strip() != str(right).strip()


def stabilize_merged_result(merged, heuristic_result, document_text=""):
    heuristic_route = str(heuristic_result.get("vec_route") or "").strip()
    if merged.get("vec_route") == "manual_review" and heuristic_route not in ("", "unknown", "manual_review"):
        merged["vec_route"] = heuristic_route

    heuristic_center_result = clean_text(heuristic_result.get("center_result_text"))
    if heuristic_center_result:
        merged["center_result_text"] = heuristic_center_result

    heuristic_course_code = clean_text(heuristic_result.get("course_code"))
    if heuristic_course_code and text_values_conflict(merged.get("course_code"), heuristic_course_code):
        merged["course_code"] = heuristic_course_code

    for key in ("start_date", "end_date"):
        merged_date = parse_numeric_date(merged.get(key)) or parse_written_date(merged.get(key))
        heuristic_date = parse_numeric_date(heuristic_result.get(key)) or parse_written_date(heuristic_result.get(key))
        if merged_date and heuristic_date and merged_date != heuristic_date:
            merged[key] = heuristic_result.get(key)

    for key in ("hours", "credits"):
        if numeric_values_conflict(merged.get(key), heuristic_result.get(key)):
            merged[key] = heuristic_result.get(key)

    merged_year = parse_result_year(merged.get("year"))
    heuristic_year = parse_result_year(heuristic_result.get("year"))
    if merged_year and heuristic_year and merged_year != heuristic_year:
        merged["year"] = heuristic_result.get("year")

    center_search_source = (
        clean_text(heuristic_result.get("center_search_text"))
        or clean_text(merged.get("center_search_text"))
        or clean_text(merged.get("institution_name"))
        or clean_text(merged.get("organizing_entity"))
        or extract_institution_name(document_text)
    )
    merged["center_search_text"] = shorten_center_name(center_search_source)

    return merged


def merge_results(heuristic_result, ai_result, document_text="", pdf_path=None):
    ai_payload = normalize_ai_result(ai_result)
    merged = blank_result()
    merged.update(ai_payload)

    route = merged.get("vec_route", "")
    if route not in ROUTE_VALUES:
        route = heuristic_result.get("vec_route", "unknown")
    if route == "unknown" and heuristic_result.get("vec_route") != "unknown":
        route = heuristic_result["vec_route"]
    merged["vec_route"] = route

    for key in (
        "merit_name",
        "merit_type",
        "professional_category",
        "organizing_entity",
        "institution_name",
        "center_name",
        "center_search_text",
        "center_result_text",
        "center_code",
        "start_date",
        "end_date",
        "scope",
        "training_type",
        "delivery_method",
        "accredited_choice",
        "accredited_received",
        "accreditation_type",
        "course_code",
        "credits_or_hours",
        "document_description",
        "description",
        "reasoning_summary",
    ):
        if merged.get(key) in (None, ""):
            merged[key] = heuristic_result.get(key, "")

    for key in BOOLEAN_RESULT_FIELDS:
        if key not in ai_payload or merged.get(key) in ("", None):
            merged[key] = heuristic_result.get(key)

    for key in LIST_RESULT_FIELDS:
        if not merged.get(key):
            merged[key] = heuristic_result.get(key, [])

    for key in ("hours", "credits", "year"):
        if merged.get(key) in (None, "", 0):
            merged[key] = heuristic_result.get(key, 0)

    if not merged.get("confidence"):
        merged["confidence"] = heuristic_result.get("confidence", 0.0)

    merged = stabilize_merged_result(
        merged,
        heuristic_result,
        document_text=document_text,
    )

    return repair_result_dates(
        finalize_result_for_vec(
            merged,
            heuristic_result,
            document_text=document_text,
            pdf_path=pdf_path,
        )
    )


def classify_document_with_openai(document_text, pdf_path, heuristic_result=None):
    system_prompt = CLIENT_GPT_INSTRUCTIONS
    heuristic_result = heuristic_result or {}
    attached_paths = build_openai_attached_paths(pdf_path)
    file_facts = build_pdf_fact_hints(document_text, pdf_path)
    duration_hint = format_duration_hint(file_facts.get("credits"), file_facts.get("hours"))
    resolved_references = resolve_official_vec_reference_pdfs()

    user_prompt = f"""
DOCUMENTO: PDF

DATOS EXTRAIDOS DIRECTAMENTE DEL PDF PRINCIPAL:
- Nombre del merito: {file_facts.get("merit_name") or ""}
- Centro / entidad: {file_facts.get("institution_name") or ""}
- Fecha inicio: {file_facts.get("start_date") or ""}
- Fecha fin: {file_facts.get("end_date") or file_facts.get("year") or ""}
- Categoria profesional mencionada: {file_facts.get("professional_category") or ""}
- Codigo de curso / expediente: {file_facts.get("course_code") or ""}
- Dato cuantificable a usar en VEC: {duration_hint}

PDF principal: {pdf_path.name}
PDFs oficiales adjuntos obligatorios:
- {resolved_references[0][1].name}
- {resolved_references[1][1].name}

Estos datos son solo ayudas de lectura directa del propio PDF.
Si cualquier ayuda entra en conflicto con el PDF principal o con los PDFs oficiales adjuntos, prioriza los archivos adjuntos.
Si el PDF contiene créditos, usa solo créditos para VEC y no rellenes horas.

Aplica estrictamente el motor VEC definido en las instrucciones del sistema.
Usa el PDF principal como fuente principal de verdad.
Consulta tambien los dos PDFs oficiales adjuntos para rutas VEC, catalogos, centros y validaciones.
Si un dato obligatorio del catalogo no es seguro o no aparece, usa manual_review y deja alerts claras.
Devuelve solo el JSON pedido.
Recuerda que en Step 4 la mayoria de valores se seleccionan en desplegables y la interfaz puede estar en espanol o en ingles.
Por eso debes devolver valores canonicos de catalogo, breves, correlacionables y seleccionables, no explicaciones.
Si el dropdown visible estuviera en ingles o en espanol, devuelve la opcion oficial equivalente que permita a la automatizacion escoger el valor correcto.
Los campos reales de Step 4 que se rellenan son exactamente:
- Tipo Formacion* -> training_type
- Metodologia de Imparticion* -> delivery_method
- Acreditada Recibida* -> accredited_choice
- Codigo Curso -> course_code
Si el PDF soporta esos cuatro campos, debes devolverlos con valor util para la automatizacion.
"""

    save_openai_request_preview(system_prompt, user_prompt, attached_paths)

    response = get_openai_client().responses.create(
        model=MODEL_NAME,
        instructions=system_prompt,
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_prompt},
                    *build_openai_file_content_items(pdf_path),
                    {
                        "type": "input_text",
                        "text": (
                            "Texto extraido del PDF principal para redundancia y OCR de respaldo:\n"
                            f"\"\"\"\n{document_text[:14000]}\n\"\"\""
                        ),
                    },
                ],
            },
        ],
        text={"format": {"type": "json_object"}},
        temperature=0.1,
        truncation="auto",
    )

    content = (response.output_text or "").strip()
    return extract_json_object_from_text(content)


def review_blocked_classification_with_openai(document_text, pdf_path, current_result, blockers):
    system_prompt = CLIENT_GPT_INSTRUCTIONS
    attached_paths = build_openai_attached_paths(pdf_path)
    resolved_references = resolve_official_vec_reference_pdfs()
    current_result_json = json.dumps(current_result or {}, indent=2, ensure_ascii=False)
    blockers_text = "\n".join(f"- {blocker}" for blocker in (blockers or [])) or "- Sin bloqueos declarados"

    user_prompt = f"""
REVISION OPERATIVA OBLIGATORIA ANTES DE DESCARTAR EL DOCUMENTO

PDF principal: {Path(pdf_path).name}
PDFs oficiales adjuntos obligatorios:
- {resolved_references[0][1].name}
- {resolved_references[1][1].name}

Este documento NO debe descartarse todavia.
Necesitas revisar de nuevo el PDF principal junto con los dos PDFs oficiales adjuntos y decidir si realmente puede automatizarse.

Bloqueos detectados en la clasificacion previa:
{blockers_text}

Resultado previo para revisar y corregir si procede:
{current_result_json}

Instrucciones para esta segunda revision:
- Relee el PDF principal y los dos PDFs oficiales adjuntos.
- No descartes por defecto.
- Si el documento soporta una clasificacion automatizable, corrige el JSON y devuelve la ruta exacta y todos los campos soportados.
- Si tras revisar cuidadosamente sigue faltando soporte documental, devuelve manual_review con alerts muy claros.
- Si hay varias rutas parecidas en el catalogo, elige la mas precisa segun los PDFs oficiales adjuntos.
- Antes de dejar course_code, training_type, delivery_method o accredited_choice vacios, confirma que realmente no aparecen ni pueden inferirse de forma segura del PDF principal.
- Devuelve solo el JSON final.
"""

    save_openai_request_preview(system_prompt, user_prompt, attached_paths)

    response = get_openai_client().responses.create(
        model=MODEL_NAME,
        instructions=system_prompt,
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_prompt},
                    *build_openai_file_content_items(pdf_path),
                    {
                        "type": "input_text",
                        "text": (
                            "Texto extraido del PDF principal para respaldo adicional:\n"
                            f"\"\"\"\n{(document_text or '')[:18000]}\n\"\"\""
                        ),
                    },
                ],
            },
        ],
        text={"format": {"type": "json_object"}},
        temperature=0.0,
        truncation="auto",
    )

    content = (response.output_text or "").strip()
    ai_result = extract_json_object_from_text(content)
    reviewed_result = merge_results(
        current_result or {},
        ai_result,
        document_text=document_text,
        pdf_path=pdf_path,
    )
    reviewed_result["source_pdf_path"] = str(pdf_path)
    reviewed_result["source_pdf_filename"] = Path(pdf_path).name
    return reviewed_result


def select_pdf_candidate(explicit_pdf_path=None, pdf_directory=PDF_DIRECTORY, backend="openai"):
    pdf_candidates = resolve_pdf_candidates(explicit_pdf_path, pdf_directory)
    skipped_candidates = []

    for candidate_path in pdf_candidates:
        print("Using PDF:", candidate_path)
        print("Extracting text from PDF...")

        try:
            candidate_text = extract_pdf_text(candidate_path)
        except Exception as exc:
            if backend == "openai":
                reason = (
                    f"Local text extraction failed for {candidate_path.name}: {exc}. "
                    "Proceeding with OpenAI file review before any skip."
                )
                print(reason)
                return candidate_path, "", skipped_candidates
            reason = f"Text extraction failed for {candidate_path.name}: {exc}"
            skip_pdf_candidate(candidate_path, reason, status="skipped_extraction_error")
            skipped_candidates.append(reason)
            continue

        if not has_extractable_text(candidate_text):
            if backend == "openai":
                reason = (
                    f"No reliable local text could be extracted from {candidate_path.name}. "
                    "Proceeding with OpenAI file review before any skip."
                )
                print(reason)
                return candidate_path, candidate_text or "", skipped_candidates
            reason = f"No text could be extracted from the PDF: {candidate_path}"
            skip_pdf_candidate(candidate_path, reason, status="skipped_no_text")
            skipped_candidates.append(reason)
            continue

        return candidate_path, candidate_text, skipped_candidates

    skipped_summary = "; ".join(skipped_candidates) or "No usable PDF candidates were found."
    raise ValueError(
        "No usable PDF with extractable text was found. "
        f"Skipped candidates: {skipped_summary}"
    )


def classify_selected_pdf(document_text, pdf_path, backend="openai"):
    heuristic_result = classify_with_heuristics(document_text, pdf_path)
    print("Text extracted successfully.")
    result = None
    backend_used = "heuristic"
    last_error = None

    if result is None and backend == "openai":
        print("Sending the VEC system engine, the merit PDF, and the official VEC reference PDFs to OpenAI...")
        try:
            ai_result = classify_document_with_openai(document_text, pdf_path, heuristic_result=heuristic_result)
            result = merge_results(
                heuristic_result,
                ai_result,
                document_text=document_text,
                pdf_path=pdf_path,
            )
            backend_used = "openai"
        except RateLimitError as exc:
            last_error = exc
            print("OpenAI quota unavailable.")
        except Exception as exc:
            last_error = exc
            print(f"OpenAI classification failed: {exc}")

    if result is None:
        if last_error:
            print(f"Using local heuristic classification after AI backend fallback failed: {last_error}")
        else:
            print("Using local heuristic classification.")
        result = repair_result_dates(heuristic_result)

    result["source_pdf_path"] = str(pdf_path)
    result["source_pdf_filename"] = pdf_path.name
    return result, heuristic_result, backend_used


def save_classification_outputs(result, document_text, output=OUTPUT_JSON_FILE, text_output=OUTPUT_TEXT_FILE):
    with open(text_output, "w", encoding="utf-8") as file_handle:
        file_handle.write(document_text)

    with open(output, "w", encoding="utf-8") as file_handle:
        json.dump(result, file_handle, indent=2, ensure_ascii=False)


def classify_next_document(
    explicit_pdf_path=None,
    pdf_directory=PDF_DIRECTORY,
    output=OUTPUT_JSON_FILE,
    text_output=OUTPUT_TEXT_FILE,
    backend="openai",
):
    pdf_path, document_text, skipped_candidates = select_pdf_candidate(
        explicit_pdf_path=explicit_pdf_path,
        pdf_directory=pdf_directory,
        backend=backend,
    )
    result, heuristic_result, backend_used = classify_selected_pdf(
        document_text=document_text,
        pdf_path=pdf_path,
        backend=backend,
    )
    save_classification_outputs(result, document_text, output=output, text_output=text_output)
    return {
        "result": result,
        "heuristic_result": heuristic_result,
        "backend_used": backend_used,
        "pdf_path": pdf_path,
        "document_text": document_text,
        "output_path": output,
        "text_output_path": text_output,
        "skipped_candidates": skipped_candidates,
    }


def main():
    args = parse_args()
    backend = str(args.classification_backend or "openai").strip().lower() or "openai"
    classification = classify_next_document(
        explicit_pdf_path=args.pdf,
        pdf_directory=args.pdf_dir,
        output=args.output,
        text_output=args.text_output,
        backend=backend,
    )
    result = classification["result"]
    backend_used = classification["backend_used"]

    print("Classification complete.")
    print(f"Classification backend used: {backend_used}")
    print()
    print("=== RESULT ===")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print()
    print(f"Saved text to: {args.text_output}")
    print(f"Saved JSON to: {args.output}")


if __name__ == "__main__":
    main()
