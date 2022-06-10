// https://github.com/mongodb-js/jsonpatch-to-mongodb
// modded to address https://github.com/mongodb-js/jsonpatch-to-mongodb/issues/6
// differently, and use $pull instead of $unset for array elements.
/* istanbul ignore file */

const pointer = require("json-pointer");

function toDot(path) {
  return path
    .replace(/^\//, "")
    .replace(/\//g, ".")
    .replace(/~1/g, "/")
    .replace(/~0/g, "~");
}

module.exports = function (patches, source) {
  var update = {};
  let parentPath, parentValue; // gongo
  patches.map(function (p) {
    switch (p.op) {
      case "add":
        var path = toDot(p.path),
          parts = path.split(".");

        var positionPart = parts.length > 1 && parts[parts.length - 1];
        var addToEnd = positionPart === "-";
        var key = parts.slice(0, -1).join(".");
        var $position = (positionPart && parseInt(positionPart, 10)) || null;

        update.$push = update.$push || {};

        if ($position !== null) {
          if (update.$push[key] === undefined) {
            update.$push[key] = {
              $each: [p.value],
              $position: $position,
            };
          } else {
            if (
              update.$push[key] === null ||
              update.$push[key].$position === undefined
            ) {
              throw new Error(
                "Unsupported Operation! can't use add op with mixed positions"
              );
            }
            var posDiff = $position - update.$push[key].$position;
            if (posDiff > update.$push[key].$each.length) {
              throw new Error(
                "Unsupported Operation! can use add op only with contiguous positions"
              );
            }
            update.$push[key].$each.splice(posDiff, 0, p.value);
            update.$push[key].$position = Math.min(
              $position,
              update.$push[key].$position
            );
          }
        } else if (addToEnd) {
          if (update.$push[key] === undefined) {
            update.$push[key] = p.value;
          } else {
            if (
              update.$push[key] === null ||
              update.$push[key].$each === undefined
            ) {
              update.$push[key] = {
                $each: [update.$push[key]],
              };
            }
            if (update.$push[key].$position !== undefined) {
              throw new Error(
                "Unsupported Operation! can't use add op with mixed positions"
              );
            }
            update.$push[key].$each.push(p.value);
          }
        } else {
          // gongo mod
          //throw new Error("Unsupported Operation! can't use add op without position");
          update.$set = update.$set || {};
          update.$set[toDot(p.path)] = p.value;
          delete update.$push;
        }
        break;
      case "remove":
        // gongo mod
        parentPath = p.path.replace(/\/[0-9]+$/, "");
        parentValue = pointer.get(source, parentPath);
        if (source && Array.isArray(parentValue)) {
          const pos = parseInt(p.path.substr(parentPath.length + 1));
          update.$set = update.$set || {};
          update.$set[toDot(parentPath)] = parentValue;
          parentValue.splice(pos, 1);

          //const $parentPath = "$" + toDot(parentPath);

          // https://jira.mongodb.org/browse/SERVER-1014
          // but requires aggregration pipeline, breaks $set for other things
          // i.e. $set: { "products.0.qty": 2 } => { products: [ 0: { qty: 2 }, qty: 1 ]}
          /*
          update.$set = update.$set || {};
          update.$set[toDot(parentPath)] = {
            $concatArrays: [
              { $slice: [$parentPath, pos] },
              {
                $slice: [
                  $parentPath,
                  { $add: [1, pos] },
                  { $size: $parentPath },
                ],
              },
            ],
          };
          */
        } else {
          // original case
          update.$unset = update.$unset || {};
          update.$unset[toDot(p.path)] = 1;
        }
        break;
      case "replace":
        update.$set = update.$set || {};
        update.$set[toDot(p.path)] = p.value;
        break;
      case "test":
        break;
      default:
        throw new Error("Unsupported Operation! op = " + p.op);
    }
  });
  return update;
};
