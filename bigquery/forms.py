from django import forms

class UploadForm(forms.Form):
    choices_tp_projetos = (
        ('', ''),
        ('USINAS', 'Usinas'),
        ('FAZENDA', 'Fazendas'),
        ('RODOVIAS','Rodovias')
        )
    
    analises = [
        ('CONCORRENCIA', 'Concorrência'),
        ('ESG', 'ESG'),
        ('ICR', 'ICR'),
        ('HA COBERTO', '% Área Coberta')
    ]

    nm_projeto = forms.CharField(max_length=100, label="Nome do Projeto:")
    tp_projeto = forms.ChoiceField(choices=choices_tp_projetos, label="Tipo de Projeto:")
    analises_req = forms.MultipleChoiceField(
    label='Análise Requerida',
    choices=analises,
    widget=forms.CheckboxSelectMultiple(attrs={'class': 'checkbox-group'}),
    required=True
    )
    arquivo = forms.FileField()


class UploadMancha(forms.Form):
    choices_operadoras = (("TIM", "Tim"), ("VIVO", "Vivo"), ("CLARO", "Claro"))
    mancha_mes_ref = forms.IntegerField(label="Mês da mancha:")
    nm_operadora = forms.ChoiceField(choices=choices_operadoras, label="Operadora:")
    arquivo = forms.FileField()