from django.urls import path, include

urlpatterns = [
    path('', include('llm_load_balancer.urls')),
]
