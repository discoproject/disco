from django.http import HttpResponseServerError
from django.views import debug
from django.views.debug import technical_500_response

from discodex.models import IndexCollection
Indices = IndexCollection()

def contextual_500_response(request, exc_type, exc_value, tb):
    from traceback import format_exception
    if 'HTTP_USER_AGENT' in request.META:
        return technical_500_response(request, exc_type, exc_value, tb)
    return HttpResponseServerError(format_exception(exc_type, exc_value, tb))
debug.technical_500_response = contextual_500_response
