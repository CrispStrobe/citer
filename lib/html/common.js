// File: lib/html/common.js

/**
 * Copies the text content of a given HTML element to the clipboard.
 * Supports special modes for partial copying of citation text.
 * @param {HTMLElement} elem The element to copy text from.
 * @param {number} [mode] Optional mode for special copy operations.
 */
function copyText(elem, mode) {
    if (!elem) return;
    let text = elem.textContent || '';
    switch (mode) {
        case 0: // sfn
        case 1: // full citation
            text = text.split('\n\n')[mode] || '';
            break;
        case 3: // self-closing ref
            text = text.slice(0, text.indexOf('>')) + '/>';
            break;
        case 4: // ref without name
            text = text.replace(/ name=".*?">/, '>');
            break;
    }
    navigator.clipboard.writeText(text);
}

/**
 * Handles changes on form inputs (dropdowns, radios).
 * Automatically submits the form to reload the page with new parameters,
 * but only if there is text in the search box.
 */
function handleFormChange() {
    const form = document.getElementById("form");
    const userInput = document.getElementById('user_input');
    if (userInput.value.trim() !== '') {
        form.submit();
    }
}

// This function runs when the page has finished loading.
document.addEventListener('DOMContentLoaded', function () {
    // Set the browser tab icon.
    document.querySelector('link[rel="icon"]').href = "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📚</text></svg>";
});