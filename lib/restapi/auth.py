from types import FunctionType, MethodType

from .resource import HttpResponseUnauthorized
from .resource import Resource

class authenticate(object):
    def __init__(self, authentication):
        self.authentication = authentication

    def __call__(self, obj):
        if isinstance(obj, type) and issubclass(obj, Resource):
            return self._authenticate_resource(obj)
        elif isinstance(obj, FunctionType):
            return self._authenticate_method(obj)
        elif isinstance(obj, MethodType):
            return self._authenticate_method(obj.im_func)
        raise TypeError('Can only authenticate a Resource or class method not %s' % obj)

    def _authenticate_resource(self, cls):
        for k, v in cls.responses.iteritems():
            setattr(cls, v, self(getattr(cls, v)))
        return cls

    def _authenticate_method(self, method):
        if not hasattr(method, 'authenticated_method'):
            def authenticated(resource, request, *args, **kwargs):
                if self.authentication.is_authenticated(request):
                    return method(resource, request, *args, **kwargs)
                return self.authentication.challenge()
            authenticated.authenticated_method = method
            return authenticated
        return method

def deauthenticate(obj):
    if isinstance(obj, type) and issubclass(obj, Resource):
        return _deauthenticate_resource(obj)
    elif isinstance(obj, FunctionType):
        return _deauthenticate_method(obj)
    elif isinstance(obj, MethodType):
        return _deauthenticate_method(obj.im_func)
    raise TypeError('Can only deauthenticate a Resource or class method not %s' % obj)

def _deauthenticate_resource(cls):
    for k, v in cls.responses.iteritems():
        setattr(cls, v, deauthenticate(getattr(cls, v)))
    return cls

def _deauthenticate_method(method):
    deauthenticated = method
    if hasattr(method, 'authenticated_method'):
        deauthenticated = method.authenticated_method
    deauthenticated.authenticated_method = None
    return deauthenticated

# Setting the authentication at the class level will never override method level settings,
# even for a subclass. Explicit deauthentication, however, applies to everything within scope.
# Since class decorators are applied after method decorators, a class marked as deauthenticated
# will not have any authenticated methods, even if they are so decorated.

# Taken from django_restapi.authentication

import time, random
from hashlib import md5
from django.utils.translation import gettext as _

def djangouser_auth(username, password):
    """
    Check username and password against
    django.contrib.auth.models.User
    """
    from django.contrib.auth.models import User
    try:
        user = User.objects.get(username=username)
        if user.check_password(password):
            return True
        else:
            return False
    except User.DoesNotExist:
        return False

class HttpBasicAuthentication(object):
    """
    HTTP/1.0 basic authentication.
    """
    def __init__(self, authfunc=djangouser_auth, realm=_('Restricted Access')):
        """
        authfunc:
            A user-defined function which takes a username and
            password as its first and second arguments respectively
            and returns True if the user is authenticated
        realm:
            An identifier for the authority that is requesting
            authorization
        """
        self.realm = realm
        self.authfunc = authfunc

    def challenge(self):
        """
        Returns the http headers that ask for appropriate
        authorization.
        """
        return HttpResponseUnauthorized('Basic realm="%s"' % self.realm)

    def is_authenticated(self, request):
        """
        Checks whether a request comes from an authorized user.
        """
        if not request.META.has_key('HTTP_AUTHORIZATION'):
            return False
        (authmeth, auth) = request.META['HTTP_AUTHORIZATION'].split(' ', 1)
        if authmeth.lower() != 'basic':
            return False
        auth = auth.strip().decode('base64')
        username, password = auth.split(':', 1)
        return self.authfunc(username=username, password=password)

class HttpDigestAuthentication(object):
    """
    HTTP/1.1 digest authentication (RFC 2617).
    Uses code from the Python Paste Project (MIT Licence).
    """
    def __init__(self, authfunc, realm=_('Restricted Access')):
        """
        authfunc:
            A user-defined function which takes a username and
            a realm as its first and second arguments respectively
            and returns the combined md5 hash of username,
            authentication realm and password.
        realm:
            An identifier for the authority that is requesting
            authorization
        """
        self.realm = realm
        self.authfunc = authfunc
        self.nonce    = {} # prevention of replay attacks

    def get_auth_dict(self, auth_string):
        """
        Splits WWW-Authenticate and HTTP_AUTHORIZATION strings
        into a dictionaries, e.g.
        {
            nonce  : "951abe58eddbb49c1ed77a3a5fb5fc2e"',
            opaque : "34de40e4f2e4f4eda2a3952fd2abab16"',
            realm  : "realm1"',
            qop    : "auth"'
        }
        """
        amap = {}
        for itm in auth_string.split(", "):
            (k, v) = [s.strip() for s in itm.split("=", 1)]
            amap[k] = v.replace('"', '')
        return amap

    def get_auth_response(self, http_method, fullpath, username, nonce, realm, qop, cnonce, nc):
        """
        Returns the server-computed digest response key.

        http_method:
            The request method, e.g. GET
        username:
            The user to be authenticated
        fullpath:
            The absolute URI to be accessed by the user
        nonce:
            A server-specified data string which should be
            uniquely generated each time a 401 response is made
        realm:
            A string to be displayed to users so they know which
            username and password to use
        qop:
            Indicates the "quality of protection" values supported
            by the server.  The value "auth" indicates authentication.
        cnonce:
            An opaque quoted string value provided by the client
            and used by both client and server to avoid chosen
            plaintext attacks, to provide mutual authentication,
            and to provide some message integrity protection.
        nc:
            Hexadecimal request counter
        """
        ha1 = self.authfunc(realm, username)
        ha2 = md5.md5('%s:%s' % (http_method, fullpath)).hexdigest()
        if qop:
            chk = "%s:%s:%s:%s:%s:%s" % (ha1, nonce, nc, cnonce, qop, ha2)
        else:
            chk = "%s:%s:%s" % (ha1, nonce, ha2)
        computed_response = md5.md5(chk).hexdigest()
        return computed_response

    def challenge(self, stale=''):
        """
        Returns the http headers that ask for appropriate
        authorization.
        """
        nonce  = md5.md5("%s:%s" % (time.time(), random.random())).hexdigest()
        opaque = md5.md5("%s:%s" % (time.time(), random.random())).hexdigest()
        self.nonce[nonce] = None
        parts = {'realm': self.realm, 'qop': 'auth',
                 'nonce': nonce, 'opaque': opaque }
        if stale:
            parts['stale'] = 'true'
        head = ", ".join(['%s="%s"' % (k, v) for (k, v) in parts.items()])
        return HttpResponseUnauthorized('Digest %s' % head)

    def is_authenticated(self, request):
        """
        Checks whether a request comes from an authorized user.
        """

        # Make sure the request is a valid HttpDigest request
        if not request.META.has_key('HTTP_AUTHORIZATION'):
            return False
        fullpath = request.META['SCRIPT_NAME'] + request.META['PATH_INFO']
        (authmeth, auth) = request.META['HTTP_AUTHORIZATION'].split(" ", 1)
        if authmeth.lower() != 'digest':
            return False

        # Extract auth parameters from request
        amap = self.get_auth_dict(auth)
        try:
            username = amap['username']
            authpath = amap['uri']
            nonce    = amap['nonce']
            realm    = amap['realm']
            response = amap['response']
            assert authpath.split("?", 1)[0] in fullpath
            assert realm == self.realm
            qop      = amap.get('qop', '')
            cnonce   = amap.get('cnonce', '')
            nc       = amap.get('nc', '00000000')
            if qop:
                assert 'auth' == qop
                assert nonce and nc
        except:
            return False

        # Compute response key
        computed_response = self.get_auth_response(request.method, fullpath, username, nonce, realm, qop, cnonce, nc)

        # Compare server-side key with key from client
        # Prevent replay attacks
        if not computed_response or computed_response != response:
            if nonce in self.nonce:
                del self.nonce[nonce]
            return False
        pnc = self.nonce.get(nonce,'00000000')
        if nc <= pnc:
            if nonce in self.nonce:
                del self.nonce[nonce]
            return False # stale = True
        self.nonce[nonce] = nc
        return True
