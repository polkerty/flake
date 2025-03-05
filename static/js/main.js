$(document).ready(function(){
    $("td[data-animal]").click(function(){
        var animal = $(this).data("animal");
        var bucket = $(this).data("bucket");
        var granularity = $("#granularity").val();
        $("#modalAnimal").text(animal);
        $("#modalBucket").text(bucket);
        $.get("/snapshots", { animal: animal, bucket: bucket, granularity: granularity }, function(data){
            var content = "<table class='table table-sm'><thead><tr><th>Snapshot</th><th>Status</th><th>Branch</th><th>Commit</th><th>Fail Stage</th><th>Logs</th></tr></thead><tbody>";
            if(data.length === 0) {
                content += "<tr><td colspan='6'>No snapshots found for this bucket.</td></tr>";
            } else {
                data.forEach(function(item){
                    content += "<tr>";
                    content += "<td>" + item.snapshot + "</td>";
                    content += "<td>" + item.status + "</td>";
                    content += "<td>" + item.branch + "</td>";
                    content += "<td><a href='https://github.com/postgres/postgres/commit/" + item.commit + "'>" + item.commit + "</td>";
                    content += "<td>" + (item.fail_stage || "") + "</td>";
                    content += "<td><a href='" + item.log_link + "' target='_blank'>View Logs</a></td>";
                    content += "</tr>";
                });
            }
            content += "</tbody></table>";
            $("#snapshotContent").html(content);
            $("#snapshotModal").modal("show");
        });
    });
});
