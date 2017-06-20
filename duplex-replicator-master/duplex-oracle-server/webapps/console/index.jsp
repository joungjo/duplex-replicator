<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<title>DUPLEX CONSOLE</title>
<style type="text/css" media="screen">
	@import url(styles/sorttable.css);
	@import url(styles/type-settings.css);
	@import url(styles/site.css);
	@import url(styles/prettify.css);
</style>

<style type="text/css">
	.schema-tr{
		border-top : #eeeeee 1px solid;
		border-bottom:#eeeeee 1px solid;
	}
	.table-td {
		
	}
</style>

<script type="text/javascript" src="js/jquery-1.8.1.min.js"></script>
<script type="text/javascript">
 	$(load);
	function load() {
		$.ajax({
			type : "post",
			url : "getSchemas.action",
			dataType : "json",
			success : function (data) {
				var shmtr = "";
				var count = 0;
				$.each(data, function(i, schema) {
					shmtr += "<tr class='schema-tr' #888><td width='100'>" + schema.name + "</td><td><table>";
					$.each(schema.tables, function(j, table) {
						shmtr += "<tr><td class='table-td' width='200'>" + table.name + "</td><td class='table-td' width='50' align='justify'>" 
							+table.status+"</td><td class='table-td' width='100' align='justify'>";
							if(table.status){
								shmtr += "<a href='javascript:void(0)' onclick='stop(\""+ schema.name + "\", \"" + table.name + "\")'>stop</a>"; 
							} else {
								shmtr += "<a href='javascript:void(0)' onclick='start(\""+ schema.name + "\", \"" + table.name + "\")'>start</a>"; 
							}
							shmtr += "&nbsp|&nbsp<a href='javascript:void(0)' onclick='reset(\""+ schema.name + "\", \"" + table.name + "\")'>reset</a>" 
							+"</td></tr>";
					});
					shmtr += "</table></td><td><a href='javascript:void(0)' onclick='add(\""+ schema.name + "\")'>add</a></td></tr>"
				});
				$("#center-table-body").append(shmtr);
			},
			failure : function() {

			}
		});
	}
 	
 	function stop(schema, table){
 		$.ajax({
 			type : "post",
			url : "stopThread.action",
			data :{schema:schema, table:table},
			dataType : "json",
			success : function(data) {
				$("#center-table-body").empty();
				load();
			},
			failure : function() {

			}
 		});
 	}
 	
 	function start(schema, table){
 		$.ajax({
 			type : "post",
			url : "startExtractor.action",
			data :{schema:schema, table:table},
			dataType : "json",
			success : function(data) {
				$("#center-table-body").empty();
				load();
			},
			failure : function() {

			}
 		});
 	}
 	
 	function reset(schema, table){
 		$.ajax({
 			type : "post",
			url : "rebuildExtractor.action",
			data :{schema:schema, table:table},
			dataType : "json",
			success : function(data) {
				$("#center-table-body").empty();
				load();
			},
			failure : function() {

			}
 		});
 	}
 	function add(schema){
 		var table = prompt("table name : ");
 		$.ajax({
 			type : "post",
			url : "addTable.action",
			data :{schema:schema, table:table},
			dataType : "json",
			success : function(data) {
				$("#center-table-body").empty();
				load();
			},
			failure : function() {

			}
 		});
 	}
</script>
</head>
<body>
	<p>
		<img src="icon/GEOVIS_logo_Blue-02.png" width='150' height='40' />
	</p>
	<hr />
	<table align='center' style="border-collapse:collapse;">
		<tbody>
			<tr >
				<td>schema</td>
				<td>
					<table>
						<tr>
							<td width='200' align='center'>table</td>
							<td width='50' align='justify'>status</td>
							<td width='100' align='center'>operation</td>
						</tr>
					</table>
				</td>
			</tr>
		</tbody>
	</table>
	<table id="center-table" align='center' style="border-collapse:collapse;">
		<tbody id="center-table-body" class="body">
		</tbody>
	</table>
</body>
</html>