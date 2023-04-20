<%@ page contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<!DOCTYPE html>
<html style="height: 100%">
<head>
    <title>search</title>
</head>
<body style="height: 100%">
<style>
    body {
        background-image: url('https://images.ali213.net/picfile/pic/2022/07/21/2022072195246794.jpg');
        background-size: contain;
    }
    label {
        color: white;
    }
    input[type="text"] {
        border-radius: 10px; /* 将输入框的四个角都设置为 10px 的圆角 */
        padding: 5px;
        border: 1px solid #ccc;
        height: 1.2rem;
        width: 30rem;
    }
    input[type="submit"] {
        padding: 5px 10px;
        background-color: #4285f4;
        color: #fff;
        border: none;
        border-radius: 20px; /* 将按钮的四个角都设置为 20px 的圆角 */
    }
</style>
<form action="/data" method="post"
      style="display: flex;
      justify-content: center;
      align-items: center;
      height: 100%">

    <div style="padding: 1.0rem">
        <label>
            Input Player Id: <input type="text" name="id" value="" checked>
        </label>
    </div>
    <div style="padding: 1.0rem">
        <label>
            <input type="submit" value="Search">
        </label>
    </div>
</form>

</body>
</html>