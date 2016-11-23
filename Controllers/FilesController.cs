using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace RAAFS.Controllers
{
    [Route("[controller]")]
    public class FilesController : Controller
    {
        private readonly ILogger<FilesController> _logger;
        private readonly IFileSystem _filesystem;
        public FilesController(ILogger<FilesController> logger, IFileSystem filesystem)
        {
            _logger = logger;
            _filesystem = filesystem;
        }

        // GET api/values
        [HttpGet("{key}")]
        public Task Get(string key)
        {
            //var rsp = HttpContext.Response;
            //var stream = _filesystem.Get(key);

            //if (stream == null)
            //{
            //    Response.StatusCode = (int)HttpStatusCode.NotFound;
            //    return;
            //}

            //var size = stream.Length;
            //Response.ContentLength = stream.Length;


            //var result = new HttpResponseMessage(HttpStatusCode.OK);
            //result.Content = stream;
            //result.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            //result.Content.Headers.ContentLength = size;
            ////result.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
            ////{
            ////    FileName = key,
            ////    Size = size
            ////};


            //return result;
            return TransferFileAsync(key, Response);
        }

        private async Task TransferFileAsync(string key, HttpResponse response)
        {
            try
            {
                using (var stream = _filesystem.Get(key))
                {
                    if (stream == null)
                    {
                        response.StatusCode = 404;
                        return;
                    }

                    response.ContentLength = stream.Length;
                    response.ContentType = "application/octet-stream";
                    var read = 0;
                    var buffer = new byte[4096];
                    while ((read = await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false)) > 0)
                    {
                        await response.Body.WriteAsync(buffer, 0, read).ConfigureAwait(false);
                        //_logger.LogInformation("Wrote: {0}", read);
                    }
                    return;
                }
            }
            catch (Exception)
            {
                return;
            }
        }

        [HttpPost("{key}")]
        public async Task<ActionResult> Put()
        {
            //using args in the function made asp.net core preprocess the body stream
            string key = System.IO.Path.GetFileName(Request.Path.Value);
            var result = await _filesystem.Put(key, Request.Body).ConfigureAwait(false);
            //_logger.LogInformation("Wrote {0}", result);

            if (result > 0)
                return Ok();

            return BadRequest();
        }
    }
}
