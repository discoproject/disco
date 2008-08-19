/*
 * Copyright (c) 2008 Greg Weber webs.dev gmail
 * Dual licensed under the MIT and GPL licenses:
 * http://www.opensource.org/licenses/mit-license.php
 * http://www.gnu.org/licenses/gpl.html
 *
 * jquery plugin
 * make an html table editable by the user
 *   user clicks on a cell, edits the value,
 *   then presses enter or clicks on any cell to save the new value
 * 
 * var t = $('table')
 * $.uiTableEdit( t ) // returns t
 *
 * options : off, mouseDown, find, dataEntered, dataVerify, editDone
 *   off : turns off table editing
 *   find : defaults to tbody > tr > td
 *   mousedown : called in context of the table cell (as a normal event would be)
 *     if mouseDown returns false, cell will not become editable
 *   dataVerify : called in context of the cell,
 *     if dataVerify returns false, cell will stay in editable state
 *     if dataVerify returns a value, that value will replace the cell's value
 *     arguments are cell's value, original text, event
 * editDone invoked on completion
 *   arguments are cell's new value, original text
*/
jQuery.uiTableEdit = function(jq, options){
  function unbind(){
    return jq.find( options.find ).unbind('mousedown.uiTableEdit')
  }
  options = options || {}
  options.find = options.find || 'tbody > tr > td'
  if( options.off ){
    unbind().find('form').each( function(){ var f = $(this);
      f.parents("td:first").text( f.find(':text').attr('value') );
      f.remove();
    });
    return jq;
  }

 function bind_mouse_down( mouseDn ){
    unbind().bind('mousedown.uiTableEdit', mouseDn )
  }
  function td_edit(){
    var td = jQuery(this);
    function restore(e){
      var val = td.find(':text').attr('value')
      if( options.dataVerify ){
        var value = options.dataVerify.call(this, val, orig_text, e);
        if( value == false ){ return false; }
        if( value != null && value != undefined ) val = value;
      }
      td.html( "" );
      td.text( val );
      if( options.editDone ) options.editDone(val,orig_text)
      bind_mouse_down( td_edit );
    }
    var orig_text = td.text();
    var w = td.width();
    var h = td.height();
    td.css({width: w + "px", height: h + "px", padding: "0", margin: "0"});
    td.html( '<form name="td-editor" action="javascript:void(0);">' +
      '<input type="text" name="td_edit" value="' +
    td.text() + '"' + ' style="margin:0px;padding:0px;border:0px;width: ' +
      w  + 'px;">' + '</input></form>' )
      .find('form').submit( restore ).mousedown(restore)

    // focus bug (seen in FF) fixed by small delay
    function focus_text(){ td.find('input:text').get(0).focus() }
    bind_mouse_down( restore );
    setTimeout(focus_text, 50);
  }

  bind_mouse_down( !options.mouseDown ? td_edit : function(){
    if( options.mouseDown.apply(this,arguments) == false ) return false;
    td_edit.apply(this,arguments);
  });
  return jq;
}
