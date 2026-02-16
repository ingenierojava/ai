import os
import pandas as pd
import time
from typing import List, Optional
from pydantic import BaseModel, Field

# Librerías de LangChain y Gemini
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser

# --- CONFIGURACIÓN ---
# Asegúrate de poner tu API KEY aquí
os.environ["GOOGLE_API_KEY"] = "Poner GOOGLE_API_KEY"

PDF_PATH = "CVE 2743431-NÓMINA DE OPERADORES DE IMPORTANCIA VITAL.pdf"
EXCEL_OUTPUT = "Nomina_OIV_Extraccion_Corregida.xlsx"

# --- DEFINICIÓN DE ESTRUCTURA DE DATOS (Pydantic) ---
class FilaEmpresa(BaseModel):
    numero: Optional[str] = Field(description="El número de lista (Nº)")
    razon_social: str = Field(description="El nombre o Razón Social de la empresa")
    rut: str = Field(description="El RUT de la empresa")
    domicilio: str = Field(description="La dirección o domicilio de la empresa")

# Creamos una estructura contenedor para UNA sola sección a la vez
class SeccionIndividual(BaseModel):
    items: List[FilaEmpresa]  = Field(description="Lista de instituciones extraídas de la sección solicitada")

# --- DICCIONARIO DE SECCIONES A EXTRAER ---
# Definimos las instrucciones específicas para cada hoja del Excel
TAREAS_EXTRACCION = [
    {
        "id": "I_Sector_Electrico",
        "descripcion": "I. Instituciones que proveen servicios de generación, transmisión o distribución eléctrica y el Coordinador Eléctrico Nacional"
    },
    {
        "id": "II_Telecomunicaciones",
        "descripcion": "II. Instituciones que prestan servicios de telecomunicaciones"
    },
    {
        "id": "III_Digital",
        "descripcion": "III. Instituciones que realizan actividades de infraestructura digital, servicios digitales y servicios de tecnología de la información"
    },
    {
        "id": "IV_Financiero",
        "descripcion": "IV. Instituciones que realizan actividades de banca, servicios financieros y medios de pago"
    },
    {
        "id": "V_Salud",
        "descripcion": "V. Instituciones que realizan servicios de prestación institucional de salud"
    },
    {
        "id": "VI_EmpresasEstado",
        "descripcion": "VI. Empresas del Estado y del sector estatal"
    },
    {
        "id": "VII_OrganismosEstado",
        "descripcion": "VII. Organismos de la Administración del Estado"
    }
]

# --- FUNCIÓN PRINCIPAL ---
def procesar_documento():
    print(f"🔄 Cargando documento: {PDF_PATH}...")
    
    try:
        loader = PyPDFLoader(PDF_PATH)
        pages = loader.load()
        full_text = "\n".join([page.page_content for page in pages])
        print("✅ Documento cargado. Iniciando extracción iterativa...")
    except Exception as e:
        print(f"❌ Error al cargar el PDF: {e}")
        return

    
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash", 
        temperature=0,
        max_retries=2
    )

    parser = PydanticOutputParser(pydantic_object=SeccionIndividual)
    
    # Diccionario para guardar los DataFrames resultantes
    resultados_dfs = {}

    # --- BUCLE DE EXTRACCIÓN ---
    for tarea in TAREAS_EXTRACCION:
        sector_id = tarea["id"]
        descripcion_busqueda = tarea["descripcion"]
        
        print(f"\n--- Procesando: {sector_id} ---")
        
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", "Eres un experto en extracción de datos legales exactos."),
            ("user", """
                Analiza el texto del documento adjunto.
                Tu ÚNICO objetivo es extraer la tabla correspondiente a la sección:
                
                **"{descripcion}"**
                
                Instrucciones Críticas:
                1. Extrae TODAS las filas de esta sección específica.
                2. No inventes datos. Si el texto está cortado, intenta reconstruirlo lógicamente.
                3. Ignora encabezados y pies de página.
                4. Devuelve SOLO el JSON estructurado según se solicita.
                
                {format_instructions}
                
                --- TEXTO DEL DOCUMENTO ---
                {text}
            """)
        ])

        chain = prompt_template | llm | parser

        try:
            # Invocamos al modelo solo para esta sección
            resultado = chain.invoke({
                "descripcion": descripcion_busqueda,
                "text": full_text,
                "format_instructions": parser.get_format_instructions()
            })
            
            # Convertimos a DataFrame
            data = [item.dict() for item in resultado.items]
            df = pd.DataFrame(data)
            resultados_dfs[sector_id] = df
            
            print(f"   ✅ Extraído con éxito: {len(df)} registros.")
            
        except Exception as e:
            print(f"   ❌ Error extrayendo {sector_id}: {e}")
            # Creamos un DF vacío para no romper el Excel final
            resultados_dfs[sector_id] = pd.DataFrame(columns=["numero", "razon_social", "rut", "domicilio"])
        
        # Pequeña pausa para no saturar la API
        time.sleep(1)

    # --- GUARDAR EN EXCEL ---
    print("\n💾 Guardando archivo Excel consolidado...")
    try:
        with pd.ExcelWriter(EXCEL_OUTPUT, engine='openpyxl') as writer:
            for sheet_name, df in resultados_dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"🎉 ¡Proceso finalizado! Archivo: {EXCEL_OUTPUT}")
        
    except Exception as e:
        print(f"❌ Error guardando el Excel: {e}")

if __name__ == "__main__":
    procesar_documento()