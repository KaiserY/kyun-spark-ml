package com.kaisery.controller

import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

@RestController
class HelloController {
    @RequestMapping(value = Array("/"), method = Array(RequestMethod.GET))
    def hello = "hello world!!"
}
