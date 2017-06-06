package yanbinwa.iOrchestration.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import yanbinwa.iOrchestration.exception.ServiceUnavailableException;
import yanbinwa.iOrchestration.service.IOrchestrationService;

@RestController
@RequestMapping("/iOrchestration")
public class OrchestrationController
{
    
    @Autowired
    IOrchestrationService iOrchestrationService;
    
    @RequestMapping(value="/getReadyService",method=RequestMethod.GET)
    public String getReadyService() throws ServiceUnavailableException 
    {
        return iOrchestrationService.getReadyService().toString();
    }
    
    @RequestMapping(value="/isServiceReady",method=RequestMethod.GET)
    public boolean isServiceReady(@RequestParam("serviceName") String serviceName) throws ServiceUnavailableException
    {
        return iOrchestrationService.isServiceReady(serviceName);
    }
    
    @RequestMapping(value="/isActiveManageService",method=RequestMethod.GET)
    public boolean isActiveManageService() throws ServiceUnavailableException
    {
        return iOrchestrationService.isActiveManageService();
    }
    
    @RequestMapping(value="/startManageService",method=RequestMethod.POST)
    public void startManageService()
    {
        iOrchestrationService.startManageService();
    }
    
    @RequestMapping(value="/stopManageService",method=RequestMethod.POST)
    public void stopManageService()
    {
        iOrchestrationService.stopManageService();
    }
    
    @ResponseStatus(value=HttpStatus.NOT_FOUND, reason="Orchestration service is stop")
    @ExceptionHandler(ServiceUnavailableException.class)
    public void serviceUnavailableExceptionHandler() 
    {
        
    }
}
