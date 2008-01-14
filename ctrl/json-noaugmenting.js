/*
    3/9/2007
    This is a modified version of the orginal json.js of Douglas Crockford
    
    The original version will break programs with improper for..in loops. See
    http://yuiblog.com/blog/2006/09/26/for-in-intrigue/    
    
    I adjusted json.js by removing the augmenting of the built-in types and repaced 
    this functionality by simple global functions called:
    - objectToJSONSString
    - arrayToJSONSString
    - booleanToJSONSString
    - dateToJSONSString
    - numberToJSONSString
    
    The documentation is adjusted as well... 
    
    This adjusted version is a reference implementation. You are free to copy, modify, or
    redistribute.
     
    json.js
    2007-08-19

    Public Domain

    This file adds these methods to JavaScript:

        objectToJSONSString
        arrayToJSONSString
        booleanToJSONSString
        dateToJSONSString
        numberToJSONSString
        string.toJSONString()
            
            These methods produce a JSON text from a JavaScript value.
            It must not contain any cyclical references. Illegal values
            will be excluded.

            The default conversion for dates is to the "dd/mm/yyyy hh:mm:ss GMT". You can
            overwrite the dateToJSONSString method with any function to get a different
            representation.
                   
            The object and array methods can take an optional whitelist
            argument. A whitelist is an array of strings. If it is provided,
            keys in objects not found in the whitelist are excluded.

        string.parseJSON(filter)
            This method parses a JSON text to produce an object or
            array. It can throw a SyntaxError exception.

            The optional filter parameter is a function which can filter and
            transform the results. It receives each of the keys and values, and
            its return value is used instead of the original value. If it
            returns what it received, then structure is not modified. If it
            returns undefined then the member is deleted.
           
            Example:

            // Parse the text. If a key contains the string 'date' then
            // convert the value to a date.

            myData = text.parseJSON(function (key, value) {
                return key.indexOf('date') >= 0 ? new Date(value) : value;
            });

   
    This is a reference implementation. You are free to copy, modify, or
    redistribute.

    Use your own copy. It is extremely unwise to load untrusted third party
    code into your pages.
*/

/*jslint evil: true */


    function arrayToJSONString(arr,w) {
        var a = [],     // The array holding the partial texts.
            i,          // Loop counter.
            l = arr.length,
            v;          // The value to be stringified.

// For each value in this array...

        for (i = 0; i < l; i += 1) {
            v = arr[i];
            switch (typeof v) {
            case 'object':

// Serialize a JavaScript object value. Ignore objects thats lack the
// toJSONString method. Due to a specification error in ECMAScript,
// typeof null is 'object', so watch out for that case.

                if (v) {
                    if (typeof v.toJSONString != null) {
                            if(typeof(v.pop)==='function') a.push( arrayToJSONString(v,w));/*okay this is a bit lame way to detect an array....*/
                            else if(typeof(v.getDate)==='function') a.push(dateToJSONString(v));/*okay this is a bit lame way to detect a date....*/
                            else a.push(objectToJSONString(v,w));/*todo*/
                    }
                } else {
                    a.push('null');
                }
                break;

            case 'string':
                a.push(v.toJSONString());
                break;
            case 'number':
                a.push(numberToJSONString(v));
                break;
            case 'boolean':
                a.push(booleanToJSONString(v));
                break;

// Values without a JSON representation are ignored.

            } 
        }

// Join all of the member texts together and wrap them in brackets.

        return '[' + a.join(',') + ']';
    };

 
    
    
    
    
    
    function booleanToJSONString(b) {
        return String(b);
    };

    
    

    dateToJSONString = function (d) {

// Eventually, this method will be based on the date.toISOString method.

        function f(n) {

// Format integers to have at least two digits.

            return n < 10 ? '0' + n : n;
        }

        return '"' + f(d.getUTCMonth() + 1) + '/' +
                f(d.getUTCDate())  + '/' +
                d.getUTCFullYear()  + ' ' +
                f(d.getUTCHours())      + ':' +
                f(d.getUTCMinutes())    + ':' +
                f(d.getUTCSeconds())    + ' GMT"';
    };


    function numberToJSONString(numm) {

// JSON numbers must be finite. Encode non-finite numbers as null.

        return isFinite(numm) ? String(numm) : 'null';
    };


    function objectToJSONString(obj,w) {
        var a = [],     // The array holding the partial texts.
            k,          // The current key.
            i,          // The loop counter.
            v;          // The current value.

// If a whitelist (array of keys) is provided, use it assemble the components
// of the object.

        if (w) {
            for (i = 0; i < w.length; i += 1) {
                k = w[i];
                if (typeof k === 'string') {
                    v = obj[k];
                    switch (typeof v) {
                    case 'object':

// Serialize a JavaScript object value. Ignore objects that lack the
// toJSONString method. Due to a specification error in ECMAScript,
// typeof null is 'object', so watch out for that case.

                        if (v) {
                            if (typeof v.toJSONString !=null) {
                                if(typeof(v.pop)==='function') a.push(k.toJSONString() + ':' + arrayToJSONString(v,w));
                                else if(typeof(v.getDate)==='function') a.push(k.toJSONString() + ':' + dateToJSONString(v));
                                else a.push(k.toJSONString() + ':' + objectToJSONString(v,w));
                                
                            }                                                      
                        } else {
                            a.push(k.toJSONString() + ':null');
                        }
                        break;

            case 'string':
                a.push(k.toJSONString() + ':' + v.toJSONString());
                break;
            case 'number':
            a.push(k.toJSONString() + ':' + numberToJSONString(v));
                break;
            case 'boolean':
                a.push(k.toJSONString() + ':' + booleanToJSONString(v));
                break;
// Values without a JSON representation are ignored.

                    }
                }
            }
        } else {

// Iterate through all of the keys in the object, ignoring the proto chain
// and keys that are not strings.

            for (k in obj) {
                if (typeof k === 'string' &&
                        Object.prototype.hasOwnProperty.apply(obj, [k])) {
                    v = obj[k];
                    switch (typeof v) {
                    case 'object':

// Serialize a JavaScript object value. Ignore objects that lack the
// toJSONString method. Due to a specification error in ECMAScript,
// typeof null is 'object', so watch out for that case.
                                 
                        if (v) {
                            if (typeof v.toJSONString != null) {  
                                    if(typeof(v.pop)==='function') a.push( arrayToJSONString(v,w));/*okay this is a bit lame way to detect an array....*/
                                    else if(typeof(v.getDate)==='function') a.push(k.toJSONString() + ':' + dateToJSONString(v));/*okay this is a bit lame way to detect an Date....*/
                                    else a.push(objectToJSONString(v,w));
                            }
                        } else {
                            a.push(k.toJSONString() + ':null');
                        }
                        break;
                       case 'string':
                          a.push(k.toJSONString() + ':' + v.toJSONString());
                           break;
                       case 'number':
                       a.push(k.toJSONString() + ':' + numberToJSONString(v));
                           break;     
                       case 'boolean':
                           a.push(k.toJSONString() + ':' + booleanToJSONString(v));
                                                                                  
// Values without a JSON representation are ignored.

                    }
                }
            }
        }

// Join all of the member texts together and wrap them in braces.

        return '{' + a.join(',') + '}';
    };

    (function (s) {

// Augment String.prototype. We do this in an immediate anonymous function to
// avoid defining global variables.
                                    
// m is a table of character substitutions.

        var m = {
            '\b': '\\b',
            '\t': '\\t',
            '\n': '\\n',
            '\f': '\\f',
            '\r': '\\r',
            '"' : '\\"',
            '\\': '\\\\'
        };


        s.parseJSON = function (filter) {
            var j;

            function walk(k, v) {
                var i;
                if (v && typeof v === 'object') {
                    for (i in v) {
                        if (Object.prototype.hasOwnProperty.apply(v, [i])) {
                            v[i] = walk(i, v[i]);
                        }
                    }
                }
                return filter(k, v);
            }


// Parsing happens in three stages. In the first stage, we run the text against
// a regular expression which looks for non-JSON characters. We are especially
// concerned with '()' and 'new' because they can cause invocation, and '='
// because it can cause mutation. But just to be safe, we will reject all
// unexpected characters.

// We split the first stage into 3 regexp operations in order to work around
// crippling deficiencies in Safari's regexp engine. First we replace all
// backslash pairs with '@' (a non-JSON character). Second we delete all of
// the string literals. Third, we look to see if only JSON characters
// remain. If so, then the text is safe for eval.

            if (/^[,:{}\[\]0-9.\-+Eaeflnr-u \n\r\t]*$/.test(this.
                    replace(/\\./g, '@').
                    replace(/"[^"\\\n\r]*"/g, ''))) {

// In the second stage we use the eval function to compile the text into a
// JavaScript structure. The '{' operator is subject to a syntactic ambiguity
// in JavaScript: it can begin a block or an object literal. We wrap the text
// in parens to eliminate the ambiguity.

                j = eval('(' + this + ')');

// In the optional third stage, we recursively walk the new structure, passing
// each name/value pair to a filter function for possible transformation.

                return typeof filter === 'function' ? walk('', j) : j;
            }

// If the text is not JSON parseable, then a SyntaxError is thrown.

            throw new SyntaxError('parseJSON');
        };


        s.toJSONString = function () {

// If the string contains no control characters, no quote characters, and no
// backslash characters, then we can simply slap some quotes around it.
// Otherwise we must also replace the offending characters with safe
// sequences.

            if (/["\\\x00-\x1f]/.test(this)) {
                return '"' + this.replace(/[\x00-\x1f\\"]/g, function (a) {
                    var c = m[a];
                    if (c) {
                        return c;
                    }
                    c = a.charCodeAt();
                    return '\\u00' +
                        Math.floor(c / 16).toString(16) +
                        (c % 16).toString(16);
                }) + '"';
            }
            return '"' + this + '"';
        };
    })(String.prototype);
