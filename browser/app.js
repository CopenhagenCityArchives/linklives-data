let from = 0;
let size = 30;

function search_next(ev) {
    from += size;
    search(from, size);
}

function search_new(ev) {
    from = 0;
    search();
}

async function search(ev) {
    var statusElement = document.getElementById("search_status");
    var resultsElement = document.getElementById("search_results");

    var firstNames = document.getElementById("form_firstnames").value;
    try {
        let json = await sendRequest("POST", "http://localhost:9200/pas,lifecourses/_search", {
            from: from,
            size: size,
            query: {
                nested: {
                    path: "person_appearance",
                    query: {
                        bool: {
                            must: [
                                {
                                    match: {
                                        "person_appearance.firstnames_std": firstNames
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        });

        let result = await JSON.parse(json);
        statusElement.innerText = `Showing  ${from + 1} to ${from + 1 + result.hits.hits.length} out of ${result.hits.total.value} records.`;
    
        resultsElement.innerHTML = "";
        result.hits.hits.forEach(hit => renderHit(hit, resultsElement));
    } catch (err) {
        console.log("error occured", err);
        statusElement.innerText = "An error occured.";
    }
}

function renderHit(hit, resultsElement) {
    let resultElement = document.createElement("li");
    let metaElement = document.createElement("div");

    renderField(metaElement, "Index", hit._index);
    renderField(metaElement, "Score", hit._score);

    resultElement.appendChild(metaElement);
    if (Array.isArray(hit._source.person_appearance)) {
        hit._source.person_appearance.forEach(pa => renderPA(pa, resultElement));
    } else {
        renderPA(hit._source.person_appearance, resultElement);
    }
    resultsElement.appendChild(resultElement);
}

function renderPA(pa, resultElement) {
    let paElement = document.createElement("div");
    paElement.classList.add("pa");
    renderField(paElement, "Name", pa.name);
    renderField(paElement, "Source",  pa.source_id);
    renderField(paElement, "Id", pa.pa_id);
    renderField(paElement, "Std firstname", pa.firstnames_std);
    renderField(paElement, "Std lastname", pa.surnames_std);
    renderField(paElement, "Std birthplace", pa.birthplace_std);
    renderField(paElement, "Std county", pa.birthplace_std);
    renderField(paElement, "Std parish", pa.birthplace_std);
    renderField(paElement, "Std district", pa.birthplace_std);
    renderField(paElement, "birthplace", pa.birthplace);
    resultElement.appendChild(paElement);
}

function renderField(parentElement, label, value) {
    if (value === undefined) {
        return;
    }
    let fieldElement = document.createElement("div");
    let labelElement = document.createElement("span");
    labelElement.classList.add("field");
    labelElement.classList.add("label");
    labelElement.innerText = label;
    let valueElement = document.createElement("span");
    valueElement.classList.add("field");
    valueElement.classList.add("value");
    valueElement.innerText = value;
    fieldElement.appendChild(labelElement);
    fieldElement.appendChild(valueElement);
    parentElement.appendChild(fieldElement);
}


function sendRequest(method, url, data) {
    return new Promise((resolve, reject) => {
        let request = new XMLHttpRequest();
        request.open(method, url, true);
        if (data) {
            request.setRequestHeader("Content-type", "application/json;charset=UTF-8");
            request.send(JSON.stringify(data));
        }
        request.onload = () => {
            if (request.status >= 200 && request.status < 300) {
                resolve(request.response);
            } else {
                reject({
                    status: request.status,
                    statusText: request.statusText
                });
            }
        };
    });
}