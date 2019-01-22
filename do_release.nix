# Use this environment to publish this package on pypi:
#
#   # Enter the environment
#   nix-shell do_release.nix
#
#   # Create the tag
#   git tag -a vx.y.z
#
#   # create the package
#   python setup.py sdist
#
#   # upload you package
#   twine upload dist/project_name-x.y.z.tar.gz

with import <nixpkgs> {};

(pkgs.python36.withPackages (ps: with ps; [twine setuptools])).env
