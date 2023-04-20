<%--
  Created by IntelliJ IDEA.
  User: junlongqiao
  Date: 4/19/23
  Time: 11:39 AM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>home</title>
  <style>
    /* 设置背景图片 */
    body {
      background-image: url("https://images.ali213.net/picfile/pic/2022/07/21/2022072195244452.jpg");
      background-size: contain;
    }

    /* 导航链接样式 */
    nav {
      display: grid;
      grid-template-columns: repeat(12,1fr);
      justify-content: space-evenly;
      align-items: center;
      width: 100%;
      height: 50px;
      background-color: transparent;
      color: #fff;
      position: fixed;
      top: 0;
      left: 0;
      z-index: 9999;
    }

    .left {
      grid-column: span 4;
      align-items: center;
      text-align: center;
      display: flex;
      flex-direction: column;
      align-items: center;
    }

    .center {
      grid-column: span 4;
      align-items: center;
      text-align: center;
      display: flex;
      flex-direction: column;
      align-items: center;
    }

    .right {
      grid-column: span 4;
      align-items: center;
      text-align: center;
      display: flex;
      flex-direction: column;
      align-items: center;
    }

    nav ul {
      display: flex;
      list-style: none;
      margin: 0;
      padding: 0;
    }

    nav li {
      margin: 0;
      padding: 0;
    }

    nav a {
      display: block;
      padding: 10px;
      text-decoration: none;
      color: #fff;
    }
    .title {
      margin-top: 15rem;
      text-align: center;
      font-size: 108px;
      font-weight: bold;
      font-style: italic;
      color: bisque;
    }
  </style>
</head>
<body>
<!-- 导航链接 -->
<nav>
  <div class="left">
    <ul>
      <li><a href="https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1489804526187744/3954607000221320/1307685531388262/latest.html">Data Analysis</a></li>
    </ul>
  </div>
  <div class="center">
    <ul>
      <li><a href="/search">Player Recommendation</a></li>
    </ul>
  </div>
  <div class="right">

    <ul>
      <li><a href="/potential">Potential Forecast</a></li>
    </ul>
  </div>
</nav>

<!-- 页面内容 -->
<div style="height: 1000px;">
  <h1 class="title">FIFA 23 Player Scouting System</h1>
</div>
</body>
</html>

