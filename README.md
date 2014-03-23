# http-dedup

Transparent reverse proxy for deduplicating HTTP requests. A request is considered a duplicate if there is there is an existing request for the same resource with exactly the same headers that the origin server has received but has not responded to.


## Rationale

http-dedup solves two different problems:

 - Impatient users retrying requests that are slow to process
 - Accidental repeat submissions
  - Double clicking on buttons or links
  - [Automatic resubmission when connection is interrupted](https://tools.ietf.org/html/rfc2616#section-8.2.4)


## Usage

    java -jar http-dedup-0.1.0-SNAPSHOT-standalone.jar [opts]
      -l, --listen [HOST:]PORT   [nil 8081]  Listen address
      -c, --connect [HOST:]PORT  [nil 8080]  Connect address
      -v, --verbose                          Verbosity level


## Performance Goals
 - Should be able to handle at least 10,000 simultaneous connections
 - Should not add more than 10ms to the handling of any request
 - Should run fine with -Xmx64m.


## Design

http-dedup uses [java.nio](http://docs.oracle.com/javase/7/docs/api/java/nio/package-summary.html) for fast, event-based networking and [core.async](https://github.com/clojure/core.async) for lightweight threading.

java.nio is used to directly to add the smallest amount of overhead to network handling. 99% of the time http-dedup is doing nothing but ferrying bytes from one socket to another, so anything happening in between needs to be the exception and not the rule.

core.async is used to create a simple sequential process for handling each connection without overwhelming the system with OS threads.

http-dedup is an inline proxy so that it can be added and removed to a running system with little or no impact. If it crashes or stops responding, Apache/nginx can automatically bypass it.


## Caveats

http-dedup is intended to work as an inline reverse proxy, so it relies on an X-Forwarded-For header being present in the request rather than considering the source of the connection.

http-dedup only considers the request head when determining if a request is duplicated. If two requests have the same Content-Length, http-dedup will consider them equivalent regardless of the actual content.


## License

[![CC0](http://i.creativecommons.org/p/zero/1.0/88x31.png)](http://creativecommons.org/publicdomain/zero/1.0/)

To the extent possible under law, the person who associated CC0 with this work has waived all copyright and related or neighboring rights to this work.
