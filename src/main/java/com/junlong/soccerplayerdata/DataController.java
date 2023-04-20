package com.junlong.soccerplayerdata;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.sql.Row;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

;
@Controller
public class DataController {

    @PostMapping(value = "/data")
    public ModelAndView getData(@RequestParam(value = "id")  String id, HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        List<Row> data;
        try  {
            data = ClusterSearch.FindCluster(Integer.parseInt(id));
        } catch (NoSuchElementException e) {
            ModelAndView modelAndView = new ModelAndView("error");
            return modelAndView;
        }
        ModelAndView modelAndView = new ModelAndView("showData");
        modelAndView.addObject("data", data);
        return modelAndView;

    }

}
