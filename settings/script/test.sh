#secondCnt=90
#while [ $secondCnt -gt 0 ];do
	#echo $secondCnt
	#sleep 1
	#let secondCnt--;
#done

echo "jyb" | sudo -S wondershaper eth0 80000 8000

sleep 8

echo "jyb" | sudo -S wondershaper clear eth0
