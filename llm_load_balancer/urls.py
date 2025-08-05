from django.urls import path
from .views import submit_request, callback_status

urlpatterns = [
    path('submit/', submit_request),
    path('callback/', callback_status),
]
