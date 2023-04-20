package com.junlong.soccerplayerdata;



import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;


;
@Controller
public class SearchController {

    @GetMapping(value = "/search")
    public String search()  {


        //ModelAndView modelAndView = new ModelAndView("search");

        return "search";

    }

}
