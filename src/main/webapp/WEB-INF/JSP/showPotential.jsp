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
        <th>Player_ID: </th>
        <th>Prediction: </th>
        <th>Short_Name: </th>
        <th>Long_Name: </th>
        <th>Player_Positions: </th>
        <th>Overall: </th>
        <th>Potential: </th>
        <th>Value_Eur: </th>
        <th>Wage_Eur: </th>
        <th>Age: </th>
        <th>Club_Name: </th>
        <th>League_Name: </th>
        <th>Nationality_Name: </th>
    </tr>
    <c:forEach var="row" items="${data}">
        <tr>
            <td><c:out value="${row.get(0)}"/></td>
            <td><c:out value="${row.get(1)}"/></td>
            <td><c:out value="${row.get(2)}"/></td>
            <td><c:out value="${row.get(3)}"/></td>
            <td><c:out value="${row.get(4)}"/></td>
            <td><c:out value="${row.get(5)}"/></td>
            <td><c:out value="${row.get(6)}"/></td>
            <td><c:out value="${row.get(7)}"/></td>
            <td><c:out value="${row.get(8)}"/></td>
            <td><c:out value="${row.get(9)}"/></td>
            <td><c:out value="${row.get(10)}"/></td>
            <td><c:out value="${row.get(11)}"/></td>
            <td><c:out value="${row.get(12)}"/></td>
        </tr>
    </c:forEach>

</table>
<style>
    table {
        width: 100%;
        max-width: 100%;
        border-collapse: collapse;
        margin-top: 15rem;
    }
    th, td, tr {
        padding: 8px;
        text-align: left;
        border-bottom: 1px solid #ddd;
        color: aqua;
    }

</style>

</body>
</html>
