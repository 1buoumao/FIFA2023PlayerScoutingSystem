<%--
  Created by IntelliJ IDEA.
  User: daimingyang
  Date: 4/18/23
  Time: 3:43 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<html>
<head>
    <title>Data</title>
</head>
<body style="background-image: url('https://images.ali213.net/picfile/pic/2022/07/21/2022072195244103.jpg');background-size: contain;">
<table align="center" border="1">
    <tr style="color: white">
        <th>Id: </th>
        <th>Short Name: </th>
        <th>Player Position: </th>
        <th>Overall: </th>
        <th>Potential: </th>
        <th>Value_eur: </th>
        <th>Wage_eur: </th>
        <th>Age: </th>
        <th>Club_name: </th>
    </tr>
<c:forEach var="row" items="${data}">
        <tr>
            <td style="color: white"><c:out value="${row.get(0)}"/></td>
            <td style="color: white"><c:out value="${row.get(1)}"/></td>
            <td style="color: white"><c:out value="${row.get(2)}"/></td>
            <td style="color: white"><c:out value="${row.get(3)}"/></td>
            <td style="color: white"><c:out value="${row.get(4)}"/></td>
            <td style="color: white"><c:out value="${row.get(5)}"/></td>
            <td style="color: white"><c:out value="${row.get(6)}"/></td>
            <td style="color: white"><c:out value="${row.get(7)}"/></td>
            <td style="color: white"><c:out value="${row.get(8)}"/></td>
        </tr>
</c:forEach>
</table>
</body>
</html>
