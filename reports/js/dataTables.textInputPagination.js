$.fn.dataTableExt.oPagination.input = {
	/*
	 * Function: oPagination.input.fnInit
	 * Purpose:  Initalise dom elements required for pagination with input textbox
	 * Returns:  -
	 * Inputs:   object:oSettings - dataTables settings object
	 *           node:nPaging - the DIV which contains this pagination control
	 *           function:fnCallbackDraw - draw function which must be called on update
	 */
	"fnInit": function ( oSettings, nPaging, fnCallbackDraw )
	{
		var nFirst = document.createElement( 'span' );
		var nPrevious = document.createElement( 'span' );
		var nNext = document.createElement( 'span' );
		var nLast = document.createElement( 'span' );
		var nInput = document.createElement( 'input' );
		var nPage = document.createElement( 'span' );
		var nOf = document.createElement( 'span' );
		
		nFirst.innerHTML = oSettings.oLanguage.oPaginate.sFirst;
		nPrevious.innerHTML = oSettings.oLanguage.oPaginate.sPrevious;
		nNext.innerHTML = oSettings.oLanguage.oPaginate.sNext;
		nLast.innerHTML = oSettings.oLanguage.oPaginate.sLast;
		
		var oClasses = oSettings.oClasses;
		nFirst.className = oClasses.sPageButton+" "+oClasses.sPageFirst;
		nPrevious.className = oClasses.sPageButton+" "+oClasses.sPagePrevious;
		nNext.className= oClasses.sPageButton+" "+oClasses.sPageNext;
		nLast.className = oClasses.sPageButton+" "+oClasses.sPageLast;
		nInput.className = "paginate_textInput";
		nOf.className = "paginate_of";
		nPage.className = "paginate_page";
		
		nInput.type = "text";
		nInput.style.width = "35px";
		nInput.style.textAlign = "right";
		nInput.style.display = "inline";
		nPage.innerHTML = "Page ";
		
		nPaging.appendChild( nFirst );
		nPaging.appendChild( nPrevious );
		nPaging.appendChild( nPage );
		nPaging.appendChild( nInput );
		nPaging.appendChild( nOf );
		nPaging.appendChild( nNext );
		nPaging.appendChild( nLast );
		
		$(nFirst).bind( 'click.DT', function () {
			if ( oSettings.oApi._fnPageChange( oSettings, "first" ) )
			{
				fnCallbackDraw( oSettings );
			}
		} );
		
		$(nPrevious).bind( 'click.DT', function() {
			if ( oSettings.oApi._fnPageChange( oSettings, "previous" ) )
			{
				fnCallbackDraw( oSettings );
			}
		} );
		
		$(nNext).bind( 'click.DT', function() {
			if ( oSettings.oApi._fnPageChange( oSettings, "next" ) )
			{
				fnCallbackDraw( oSettings );
			}
		} );
		
		$(nLast).bind( 'click.DT', function() {
			if ( oSettings.oApi._fnPageChange( oSettings, "last" ) )
			{
				fnCallbackDraw( oSettings );
			}
		} );
		
		$(nInput).keyup( function (e) {
			
			if ( e.which == 38 || e.which == 39 )
			{
				this.value++;
			}
			else if ( (e.which == 37 || e.which == 40) && this.value > 1 )
			{
				this.value--;
			}
			
			if ( this.value == "" || this.value.match(/[^0-9]/) )
			{
				/* Nothing entered or non-numeric character */
				return;
			}
			
			var iNewStart = oSettings._iDisplayLength * (this.value - 1);
			if ( iNewStart > oSettings.fnRecordsDisplay() )
			{
				/* Display overrun */
				oSettings._iDisplayStart = (Math.ceil((oSettings.fnRecordsDisplay()-1) / 
					oSettings._iDisplayLength)-1) * oSettings._iDisplayLength;
				fnCallbackDraw( oSettings );
				return;
			}
			
			oSettings._iDisplayStart = iNewStart;
			fnCallbackDraw( oSettings );
		} );
		
		/* Take the brutal approach to cancelling text selection */
		$('span', nPaging)
			.bind( 'mousedown.DT', function () { return false; } )
			.bind( 'selectstart.DT', function () { return false; } );
		
		if ( oSettings.sTableId !== '' && typeof oSettings.aanFeatures.p == "undefined" )
		{
			nPaging.setAttribute( 'id', oSettings.sTableId+'_paginate' );
			nFirst.setAttribute( 'id', oSettings.sTableId+'_first' );
			nPrevious.setAttribute( 'id', oSettings.sTableId+'_previous' );
			nNext.setAttribute( 'id', oSettings.sTableId+'_next' );
			nLast.setAttribute( 'id', oSettings.sTableId+'_last' );
		}
	},
	
	/*
	 * Function: oPagination.input.fnUpdate
	 * Purpose:  Update the input element
	 * Returns:  -
	 * Inputs:   object:oSettings - dataTables settings object
	 *           function:fnCallbackDraw - draw function which must be called on update
	 */
	"fnUpdate": function ( oSettings, fnCallbackDraw )
	{
		if ( !oSettings.aanFeatures.p )
		{
			return;
		}
		var iPages = Math.ceil((oSettings.fnRecordsDisplay()) / oSettings._iDisplayLength);
		var iCurrentPage = Math.ceil(oSettings._iDisplayStart / oSettings._iDisplayLength) + 1;
		var oClasses = oSettings.oClasses;
		
		/* Loop over each instance of the pager */
		var an = oSettings.aanFeatures.p;
		for ( var i=0, iLen=an.length ; i<iLen ; i++ )
		{
			var spans = an[i].getElementsByTagName('span');
			var inputs = an[i].getElementsByTagName('input');
			spans[3].innerHTML = " of "+iPages
			inputs[0].value = iCurrentPage;
			anStatic = [
				spans[0], spans[1], spans[4], spans[5]
			];
			$(anStatic).removeClass( oClasses.sPageButton+" "+oClasses.sPageButtonActive+" "+oClasses.sPageButtonStaticDisabled );
			if ( iCurrentPage == 1 )
			{
				anStatic[0].className += " "+oClasses.sPageButtonStaticDisabled;
				anStatic[1].className += " "+oClasses.sPageButtonStaticDisabled;
			}
			else
			{
				anStatic[0].className += " "+oClasses.sPageButton;
				anStatic[1].className += " "+oClasses.sPageButton;
			}
			
			if ( iPages === 0 || iCurrentPage == iPages || oSettings._iDisplayLength == -1 )
			{
				anStatic[2].className += " "+oClasses.sPageButtonStaticDisabled;
				anStatic[3].className += " "+oClasses.sPageButtonStaticDisabled;
			}
			else
			{
				anStatic[2].className += " "+oClasses.sPageButton;
				anStatic[3].className += " "+oClasses.sPageButton;
			}
		}
	}
}


/* Example initialisation */
$(document).ready(function() {
	$('#example').dataTable( {
		"sPaginationType": "input"
	} );
} );