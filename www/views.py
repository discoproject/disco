from django.conf import settings
from django.http import HttpResponse, HttpResponseServerError, Http404
from django.shortcuts import render_to_response
from django.template import loader, RequestContext, TemplateDoesNotExist

from models import IndexCollection

def request_context_view(view):
    def decorated_view(request, *args, **kwargs):
        template, context = view(request, *args, **kwargs)
        return render_to_response(template, RequestContext(request, context))
        try:
            template = loader.get_template(template)
            return HttpResponse(template.render(RequestContext(request, context)))
        except TemplateDoesNotExist:
            raise Http404
    return decorated_view

def titled_view(view):
    @request_context_view
    def decorated_view(request, *args, **kwargs):
        template, context =  view(request, *args, **kwargs)
        context['title'] = view.__name__
        return template, context
    return decorated_view

@titled_view
def home(request):
    return 'home.html', {}

indices = IndexCollection()


from django.views import debug
from django.views.debug import technical_500_response

def contextual_500_response(request, exc_type, exc_value, tb):
    from traceback import format_exception
    if 'HTTP_USER_AGENT' in request.META:
        return technical_500_response(request, exc_type, exc_value, tb)
    return HttpResponseServerError(format_exception(exc_type, exc_value, tb))
debug.technical_500_response = contextual_500_response
