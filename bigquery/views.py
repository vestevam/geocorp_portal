# views.py
from django.shortcuts import render, redirect
from .forms import UploadForm, UploadMancha
from . import scripts
from django.core.files.storage import default_storage
from django.contrib import messages
import google.auth
from google.cloud import storage, bigquery
from django.conf import settings
import geopandas as gpd
from io import BytesIO
from shapely import wkt
import uuid


def upload_view(request):
    if request.method == 'POST':
        form = UploadForm(request.POST, request.FILES)
        if form.is_valid():
            form_data = {
                'nm_projeto': form.cleaned_data['nm_projeto'],
                'tp_projeto': form.cleaned_data['tp_projeto'],
                'analises_req': form.cleaned_data['analises_req'],
                'arquivo': request.FILES['arquivo'],
                # Adicione outros campos conforme necessário
            }

        try:
            scripts.upload(form_data)
            scripts.fazenda_ha(form_data)
            
            # Aqui você pode salvar os metadados no banco de dados se necessário
            # Exemplo: FileModel.objects.create(nome=nome, descricao=descricao, caminho=file_path)

            messages.success(request, 'Arquivo enviado com sucesso!')
            return redirect('/upload/deucerto/')
        
        except Exception as e:
            messages.error(request, f"Erro no processamento: {str(e)}")
            # Log do erro para depuração
            print(f"ERRO NO PROCESSAMENTO: {str(e)}")
    else:
        form = UploadForm()
    
    return render(request, 'upload_template.html', {'form': form})

def deu_certo(request):
    return (render(request, "upload_sucesso.html"))

def upload_mancha(request):
    if request.method == 'POST':
        form = UploadMancha(request.POST, request.FILES)
        if form.is_valid():
            # Processa dados do formulário
            nm_operadora = form.cleaned_data['nm_operadora']
            mancha_mes_ref = form.cleaned_data['mancha_mes_ref']
            arquivo = request.FILES['arquivo']
            
            messages.success(request, 'Arquivo enviado com sucesso!')
            
            # Aqui você pode salvar os metadados no banco de dados se necessário
            # Exemplo: FileModel.objects.create(nome=nome, descricao=descricao, caminho=file_path)
            
            # Usar ADC explicitamente
            credentials, project_id = google.auth.default()

            # Solução 1: Usando o cliente do GCS diretamente (recomendado)
            client = storage.Client(
                credentials=credentials,
                project=settings.GS_PROJECT_ID
            )
            
            bucket = client.bucket(settings.GS_BUCKET_NAME)
            blob = bucket.blob(f"geocorp/portal/uploads/{arquivo.name}")
            arquivo.seek(0)
            blob.upload_from_file(arquivo)

            # PARTE MODIFICADA - FIM
            query = f"""-- Cria uma tabela externa (os dados permanecem no Cloud Storage)
                CREATE OR REPLACE EXTERNAL TABLE dm_temp.ext_tmp_macha
                OPTIONS (
                format = 'PARQUET',
                uris = ['gs://tim-sdbx-corporate-baa7-temp-data/geocorp/portal/uploads/{arquivo.name}']
                );
                
                insert dm_temp.geocorp_mancha
                select * from dm_temp.ext_tmp_macha;
                drop table dm_temp.ext_tmp_macha
                """
            
            client = bigquery.Client()  # Ou apenas client = bigquery.Client() se usar variável de ambiente
            client.query(query)

        messages.success(request, 'Arquivo enviado com sucesso!')
        return redirect('/upload/deucerto/')
    else:
        form = UploadMancha()

    return render(request, 'upload_mancha.html', {'form': form})