from django.shortcuts import render
from django.http import HttpResponse


def homepage(request):

    return HttpResponse("hi mani welcone to django app")

# Create your views here.
