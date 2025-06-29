from django.urls import path
from . import views

urlpatterns = [
    path('upload/', views.upload_view, name='upload-view'),
    path('upload/mancha/', views.upload_mancha, name='upload-mancha'),
    path('upload/deucerto/', views.deu_certo, name='deu-certo'),
]

