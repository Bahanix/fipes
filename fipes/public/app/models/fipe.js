(function() {

    App.Models.Fipe = Backbone.Model.extend({
        urlRoot: '/fipes',

        open: function(callback) {
            // FIXME: use the line below instead of the ugly
            // one. Before that, you'll need to find how to use
            // websockets with Nginx as a reverse proxy.
            //
            // var uri = "ws://" + location.host + this.url();
            var uri = 'ws://localhost:3473' + this.url();

            if ("MozWebSocket" in window) {
                WebSocket = MozWebSocket;
            }

            if ("WebSocket" in window) {
                this.ws = new WebSocket(uri);
            } else {
                throw new Error('Your browser does not support websockets');
            }

            // Code taken from cowboy_examples which was taken from
            // misultin.
            var that = this;
            this.ws.onopen = function() {
                // websocket is connected
                console.log("websocket connected!");
                // send hello data to server.
            };
            this.ws.onmessage = function (evt) {
                var event = tnetstrings.parse(evt.data).value;

                switch (event.type) {
                case "uid":
                    if (callback) callback(event.uid);
                    break;
                case "stream":
                    that.stream(that.ws, event.file, event.downloader);
                    break;
                }

                var receivedMsg = evt.data;
                console.log("server sent the following: '" + receivedMsg + "'");
            };
            this.ws.onclose = function() {
                // websocket was closed
                console.log("websocket was closed");
            };
        },

        stream: function(ws, fileId, downloader) {
            var file   = App.Files.get(fileId).get('obj');
            var reader = new FileReader;
            var seek   = 0;
            var slice  = 1024 * 512; // 512 KB

            reader.onload = function(evt) {
                var data = btoa(evt.target.result);
                var event = tnetstrings.dump({
                    type       : "chunk",
                    payload    : data,
                    downloader : downloader
                });
                ws.send(event);

                seek += slice;

                // Continue to stream the file.
                if (seek < file.size) {
                    var blob = file.mozSlice(seek, seek + slice);
                    reader.readAsBinaryString(blob);
                // Stop the stream
                } else {
                    var eos = tnetstrings.dump({
                        type: "eos",
                        downloader: downloader
                    });
                    ws.send(eos);
                }
            }

            // FIXME: mozSlice? not very portable, isn't it?
            var blob = file.mozSlice(seek, seek + slice);
            reader.readAsBinaryString(blob);
        },
    });

})();